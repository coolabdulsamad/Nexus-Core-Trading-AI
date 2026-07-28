"""
src/backtester/engine.py  (v2)
Honest backtesting:
- MetaLearner runs in mode='backtest' -> memory search is time-filtered,
  no look-ahead, no future sentiment.
- Conviction gate |prob-0.5| works for BOTH longs and shorts (the old
  confidence>=X gate made strong shorts impossible).
- Signal QUALITY gates entry and drives position size tier.
- SL/TP detection uses bar high/low with fills at stop/limit prices.
- SMA exit is buffered + confirmed (config), labels are correct.
"""
import psycopg2
import pandas as pd
import numpy as np
from config.settings import config
from src.utils.logger import setup_logger
from src.backtester.risk_gate import RiskGate
from src.backtester.broker_simulator import BrokerSimulator
from src.models.meta_learner import MetaLearner

logger = setup_logger("BacktesterEngine", "logs/backtester.log")

BAR_SECONDS = config.BAR_MINUTES * 60


class BacktesterEngine:
    def __init__(self, symbol: str, initial_capital: float = 100000.0,
                 start_date: str = "2026-03-24", end_date: str = "2026-06-24"):
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.conn = psycopg2.connect(config.database.url)
        self.risk = RiskGate(initial_capital)
        self.broker = BrokerSimulator(initial_capital)
        self.meta_learner = MetaLearner()
        self.df = None
        self.cooldown = 0

    def fetch_data(self) -> pd.DataFrame:
        df = pd.read_sql("""
            SELECT m.time_bucket AS timestamp, m.open, m.high, m.low, m.close, m.volume, m.vwap,
                   f.rsi_14, f.macd_line, f.macd_signal, f.macd_hist,
                   f.bb_pct_b, f.bb_width, f.atr_14, f.atr_pct,
                   f.volume_profile_ratio, f.vol_z, f.ret_1, f.ret_3, f.ret_12,
                   f.adx_14, f.dist_sma50, f.dist_sma200, f.dist_vwap,
                   f.hour_sin, f.hour_cos, f.sentiment_score, f.regime_label
            FROM market_data m
            JOIN feature_cache f ON m.symbol = f.symbol AND m.time_bucket = f.time_bucket
            WHERE m.symbol = %s AND m.time_bucket BETWEEN %s AND %s
            ORDER BY m.time_bucket ASC
        """, self.conn, params=(self.symbol, self.start_date, self.end_date))
        if df.empty:
            logger.error(f"No data for {self.symbol}")
            return df

        df['sma_200'] = df['close'].rolling(200).mean()
        df['atr_50_avg'] = df['atr_14'].rolling(50).mean()
        df['volatility_ratio'] = df['atr_14'] / df['atr_50_avg']
        df = df.dropna(subset=['sma_200', 'atr_50_avg'])
        logger.info(f"Loaded {len(df)} candles for {self.symbol}")
        return df

    # ------------------------------------------------------------------
    def _sma_exit_confirmed(self, idx, direction) -> bool:
        """Buffered + confirmed SMA cross exit."""
        if not config.SMA_EXIT_ENABLED:
            return False
        need = config.SMA_EXIT_CONFIRM_BARS
        if idx < need:
            return False
        buf = config.SMA_EXIT_BUFFER_ATR
        for j in range(idx - need + 1, idx + 1):
            r = self.df.iloc[j]
            if direction == 'LONG' and not (r['close'] < r['sma_200'] - buf * r['atr_14']):
                return False
            if direction == 'SHORT' and not (r['close'] > r['sma_200'] + buf * r['atr_14']):
                return False
        return True

    # ------------------------------------------------------------------
    def run(self):
        self.df = self.fetch_data().reset_index(drop=True)
        if self.df.empty:
            return
        logger.info(f"Starting honest AI backtest for {self.symbol}...")

        for idx, row in self.df.iterrows():
            price = row['close']
            t = row['timestamp']
            atr = row['atr_14']

            self.risk.reset_daily_if_new_day(t)
            equity = self.risk.capital = self.broker.get_total_equity(price)

            # 1. Cooldown
            if self.cooldown > 0:
                self.cooldown -= 1
                self.broker.check_positions(t, row['high'], row['low'], price, atr)
                self._log_equity(t, price, 'HOLD', 0.5, 0.0)
                continue

            # 2. SL/TP + trailing (intrabar)
            self.broker.check_positions(t, row['high'], row['low'], price, atr)
            if self.broker.closed_trades:
                last = self.broker.closed_trades[-1]
                if last.get('_counted') is not True:
                    self.risk.record_realized_pnl(last['pnl'])
                    last['_counted'] = True

            # 3. AI signal (point-in-time)
            result = self.meta_learner.get_signal(self.symbol, row, timestamp=t, mode='backtest')
            signal, prob = result['signal'], result['confidence']
            quality = result.get('quality', 0.0)

            # 4. Gates
            vol_ok = True
            if config.VOLATILITY_FILTER_ENABLED:
                vol_ok = row['volatility_ratio'] < config.VOLATILITY_RATIO_MAX
            conviction_ok = abs(prob - 0.5) >= config.ENTRY_CONVICTION_MARGIN
            quality_ok = quality >= config.MIN_SIGNAL_QUALITY
            can_trade, day_reason = self.risk.can_open_new_position(equity)

            # 5. Entry (flat only)
            if len(self.broker.open_positions) == 0:
                if vol_ok and conviction_ok and quality_ok and can_trade:
                    if signal == 'BUY' and price > row['sma_200']:
                        size, sl, tp = self.risk.size_with_tier(price, atr, quality, 'LONG')[:3]
                        if size > 0:
                            self.broker.open_long(t, self.risk.apply_slippage(price, True), size, sl, tp)
                            logger.info(f"BUY ({result.get('reason','')}) q={quality}")
                    elif signal == 'SELL' and price < row['sma_200']:
                        size, sl, tp = self.risk.size_with_tier(price, atr, quality, 'SHORT')[:3]
                        if size > 0:
                            self.broker.open_short(t, self.risk.apply_slippage(price, True), size, sl, tp)
                            logger.info(f"SELL ({result.get('reason','')}) q={quality}")
            else:
                # 6. Exit management
                pos = self.broker.open_positions[0]
                exit_reason = None
                if pos['type'] == 'LONG':
                    if signal == 'SELL':
                        exit_reason = "signal_flip"
                    elif self._sma_exit_confirmed(idx, 'LONG'):
                        exit_reason = "sma_cross"
                else:
                    if signal == 'BUY':
                        exit_reason = "signal_flip"
                    elif self._sma_exit_confirmed(idx, 'SHORT'):
                        exit_reason = "sma_cross"

                bar_age = (t - pos['entry_time']).total_seconds() / BAR_SECONDS
                if exit_reason is None and bar_age >= config.TIME_LIMIT_BARS:
                    exit_reason = "time_limit"

                if exit_reason:
                    exit_price = self.risk.apply_slippage(price, is_entry=False)
                    for p in self.broker.open_positions[:]:
                        self.broker._close_position(p, exit_price, t, exit_reason)
                    self.broker.open_positions.clear()
                    self.risk.record_realized_pnl(self.broker.closed_trades[-1]['pnl'])
                    self.broker.closed_trades[-1]['_counted'] = True
                    self.cooldown = config.COOLDOWN_BARS

            self._log_equity(t, price, signal, prob, quality)

        # Force close at end
        if self.broker.open_positions:
            last = self.df.iloc[-1]
            self.broker.force_close_all(last['timestamp'], last['close'])

        self.conn.close()
        self._print_summary()

    # ------------------------------------------------------------------
    def _log_equity(self, t, price, signal, prob, quality):
        self.broker.equity_curve.append({
            'timestamp': t,
            'equity': self.broker.get_total_equity(price),
            'open_positions': len(self.broker.open_positions),
            'ai_signal': signal, 'ai_confidence': prob, 'quality': quality,
            'cooldown': self.cooldown,
        })

    # ------------------------------------------------------------------
    def _print_summary(self):
        if not self.broker.equity_curve:
            return
        final = self.broker.equity_curve[-1]['equity']
        init = self.broker.initial_capital
        ret_pct = (final - init) / init * 100

        eq = pd.Series([e['equity'] for e in self.broker.equity_curve])
        rets = eq.pct_change().dropna()
        sharpe = (rets.mean() / rets.std() * np.sqrt(78 * 252)) if len(rets) > 1 and rets.std() != 0 else 0
        max_dd = ((eq.cummax() - eq) / eq.cummax()).max() * 100

        trades = self.broker.closed_trades
        wins = [t for t in trades if t['pnl'] > 0]
        losses = [t for t in trades if t['pnl'] <= 0]
        win_rate = len(wins) / len(trades) * 100 if trades else 0

        print("\n" + "=" * 60)
        print(f"HONEST AI BACKTEST: {self.symbol}")
        print("=" * 60)
        print(f"Period:         {self.start_date} -> {self.end_date}")
        print(f"Initial:        ${init:,.2f}   Final: ${final:,.2f}   Return: {ret_pct:+.2f}%")
        print(f"Sharpe:         {sharpe:.2f}   Max Drawdown: {max_dd:.2f}%")
        print("-" * 60)
        print(f"Trades: {len(trades)}   Win rate: {win_rate:.1f}%   W/L: {len(wins)}/{len(losses)}")
        if trades:
            avg_w = np.mean([t['pnl'] for t in wins]) if wins else 0
            avg_l = np.mean([t['pnl'] for t in losses]) if losses else 0
            pf = abs(sum(t['pnl'] for t in wins) / sum(t['pnl'] for t in losses)) if losses and sum(t['pnl'] for t in losses) != 0 else float('inf')
            print(f"Avg win: ${avg_w:,.2f}   Avg loss: ${avg_l:,.2f}   Profit factor: {pf:.2f}")
            # Exit-reason breakdown (this is where sma_cross gets exposed)
            print("-" * 60)
            print("EXIT REASON BREAKDOWN:")
            reasons = {}
            for t in trades:
                r = reasons.setdefault(t['reason'], {'n': 0, 'pnl': 0.0, 'wins': 0})
                r['n'] += 1
                r['pnl'] += t['pnl']
                r['wins'] += 1 if t['pnl'] > 0 else 0
            for reason, r in sorted(reasons.items(), key=lambda x: x[1]['pnl']):
                print(f"  {reason:<16} n={r['n']:<4} win%={r['wins']/r['n']*100:5.1f}  pnl=${r['pnl']:+,.2f}")
        print("=" * 60)

        pd.DataFrame(self.broker.equity_curve).to_csv(f"logs/equity_ai_{self.symbol}.csv", index=False)


if __name__ == "__main__":
    import sys
    symbol = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    start = sys.argv[2] if len(sys.argv) > 2 else "2026-03-24"
    end = sys.argv[3] if len(sys.argv) > 3 else "2026-06-24"
    BacktesterEngine(symbol, start_date=start, end_date=end).run()
