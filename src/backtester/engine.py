"""
src/backtester/engine.py
Simplified: Only AI signal + 200-SMA + trailing stops + cooldown + time exit.
"""
import psycopg2
import pandas as pd
import numpy as np
from config.settings import config
from utils.logger import setup_logger
from src.backtester.risk_gate import RiskGate
from src.backtester.broker_simulator import BrokerSimulator
from src.models.meta_learner import MetaLearner

logger = setup_logger("BacktesterEngine", "logs/backtester.log")

# --- Configuration ---
COOLDOWN_BARS = 2
# ENTRY_CONFIDENCE_MIN = 0.38    # align with BASE_ENTRY_CONFIDENCE
# TIME_LIMIT_BARS = 24           # let winners run (was 12)

ENTRY_CONFIDENCE_MIN = 0.35       # match BASE_ENTRY_CONFIDENCE
TIME_LIMIT_BARS = 18              # let trades run a bit longer (was 12)

class BacktesterEngine:
    def __init__(self, symbol: str, initial_capital: float = 100000.0, start_date: str = "2026-03-24", end_date: str = "2026-06-24"):
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.conn = psycopg2.connect(config.database.url)
        
        self.risk = RiskGate(initial_capital)
        self.broker = BrokerSimulator(initial_capital)
        
        logger.info("Initializing MetaLearner AI...")
        self.meta_learner = MetaLearner()
        
        self.df = None
        self.cooldown = 0

    def fetch_data(self) -> pd.DataFrame:
        query = """
            SELECT 
                m.time_bucket as timestamp,
                m.open, m.high, m.low, m.close, m.volume, m.vwap,
                f.rsi_14, f.macd_line, f.macd_signal, f.atr_14, f.bb_upper, f.bb_lower,
                f.volume_profile_ratio, f.sentiment_score
            FROM market_data m
            JOIN feature_cache f ON m.symbol = f.symbol AND m.time_bucket = f.time_bucket
            WHERE m.symbol = %s 
              AND m.time_bucket BETWEEN %s AND %s
            ORDER BY m.time_bucket ASC
        """
        df = pd.read_sql(query, self.conn, params=(self.symbol, self.start_date, self.end_date))
        if df.empty:
            logger.error(f"No data found for {self.symbol}")
            return df
        
        # 200-period SMA (trend filter)
        df['sma_200'] = df['close'].rolling(window=200).mean()
        
        # Volatility Ratio (optional)
        df['atr_50_avg'] = df['atr_14'].rolling(window=50).mean()
        df['volatility_ratio'] = df['atr_14'] / df['atr_50_avg']
        
        df = df.dropna(subset=['sma_200', 'atr_50_avg'])
        
        logger.info(f"Loaded {len(df)} candles for {self.symbol}")
        return df

    def run(self):
        self.df = self.fetch_data()
        if self.df.empty:
            return

        logger.info(f"Starting AI Backtest (simplified) for {self.symbol}...")

        for index, row in self.df.iterrows():
            current_price = row['close']
            current_time = row['timestamp']
            atr = row['atr_14']

            # 1. Cooldown
            if self.cooldown > 0:
                self.cooldown -= 1
                self.broker.check_positions(current_time, current_price, atr)
                total_equity = self.broker.get_total_equity(current_price)
                self.broker.equity_curve.append({
                    'timestamp': current_time,
                    'equity': total_equity,
                    'open_positions': len(self.broker.open_positions),
                    'ai_signal': 'HOLD',
                    'ai_confidence': 0.0,
                    'cooldown': self.cooldown
                })
                continue

            # 2. Check SL/TP + trailing
            closed_any = self.broker.check_positions(current_time, current_price, atr)

            # 3. AI Signal
            result = self.meta_learner.get_signal(self.symbol, row, timestamp=current_time)
            signal = result['signal']
            confidence = result['confidence']

            # 4. Volatility filter (if enabled)
            is_volatility_ok = True
            if config.VOLATILITY_FILTER_ENABLED:
                is_volatility_ok = row['volatility_ratio'] < 3.0

            # 5. Entry conditions: signal + 200-SMA + confidence + volatility
            confidence_ok = confidence >= ENTRY_CONFIDENCE_MIN

            position_type = None
            if len(self.broker.open_positions) > 0:
                position_type = self.broker.open_positions[0]['type']

            # 6. Entry logic (only if flat)
            if len(self.broker.open_positions) == 0:
                if is_volatility_ok and confidence_ok:
                    # LONG: signal BUY and price above 200-SMA
                    if signal == 'BUY' and current_price > row['sma_200']:
                        size, sl, tp = self.risk.calculate_long_position_size(current_price, atr)
                        if size > 0:
                            exec_price = self.risk.apply_slippage(current_price, is_entry=True)
                            self.broker.open_long(current_time, exec_price, size, sl, tp)
                            self.risk.capital = self.broker.get_total_equity(current_price)
                            logger.info(f"AI BUY triggered (above 200-SMA). Conf: {confidence:.4f}")
                    
                    # SHORT: signal SELL and price below 200-SMA
                    elif signal == 'SELL' and current_price < row['sma_200']:
                        size, sl, tp = self.risk.calculate_short_position_size(current_price, atr)
                        if size > 0:
                            exec_price = self.risk.apply_slippage(current_price, is_entry=True)
                            self.broker.open_short(current_time, exec_price, size, sl, tp)
                            self.risk.capital = self.broker.get_total_equity(current_price)
                            logger.info(f"AI SELL triggered (below 200-SMA). Conf: {confidence:.4f}")

            else:
                # 7. Position management: exit on signal flip or 200-SMA cross
                pos = self.broker.open_positions[0]
                exit_signal = False
                if pos['type'] == 'LONG':
                    if signal == 'SELL' or current_price < row['sma_200']:
                        exit_signal = True
                elif pos['type'] == 'SHORT':
                    if signal == 'BUY' or current_price > row['sma_200']:
                        exit_signal = True

                # Time limit exit
                bar_age = (current_time - pos['entry_time']).total_seconds() / 300
                if bar_age >= TIME_LIMIT_BARS:
                    exit_signal = True
                    logger.info(f"Time limit reached for {self.symbol}, closing.")

                if exit_signal:
                    exit_price = self.risk.apply_slippage(current_price, is_entry=False)
                    for pos in self.broker.open_positions[:]:
                        self.broker._close_position(pos, exit_price, current_time,
                                                     "AI_Exit_or_Trend_Break" if not exit_signal else "TIME_LIMIT")
                    self.broker.open_positions.clear()
                    self.risk.capital = self.broker.get_total_equity(current_price)
                    self.cooldown = COOLDOWN_BARS
                    logger.info(f"Closed {pos['type']} due to exit condition. Cooldown: {self.cooldown} bars.")

            # 8. Log equity
            total_equity = self.broker.get_total_equity(current_price)
            self.broker.equity_curve.append({
                'timestamp': current_time,
                'equity': total_equity,
                'open_positions': len(self.broker.open_positions),
                'ai_signal': signal,
                'ai_confidence': confidence,
                'cooldown': self.cooldown
            })

        # Force close everything at the end
        final_price = self.df.iloc[-1]['close']
        final_time = self.df.iloc[-1]['timestamp']
        if len(self.broker.open_positions) > 0:
            self.broker.force_close_all(final_time, final_price)

        self.conn.close()
        self._print_summary()

    def _print_summary(self):
        if not self.broker.equity_curve:
            return

        final_equity = self.broker.equity_curve[-1]['equity']
        total_return_pct = ((final_equity - self.broker.initial_capital) / self.broker.initial_capital) * 100

        equity_series = pd.Series([e['equity'] for e in self.broker.equity_curve])
        returns = equity_series.pct_change().dropna()
        sharpe_ratio = (returns.mean() / returns.std() * np.sqrt(78 * 252)) if len(returns) > 1 and returns.std() != 0 else 0

        total_trades = len(self.broker.closed_trades)
        winning_trades = [t for t in self.broker.closed_trades if t['pnl'] > 0]
        losing_trades = [t for t in self.broker.closed_trades if t['pnl'] <= 0]
        win_rate = (len(winning_trades) / total_trades * 100) if total_trades > 0 else 0

        print("\n" + "="*55)
        print(f"AI BACKTEST (SIMPLIFIED): {self.symbol}")
        print("="*55)
        print(f"Start Date:    {self.start_date}")
        print(f"End Date:      {self.end_date}")
        print(f"Initial Capital: ${self.broker.initial_capital:,.2f}")
        print(f"Final Equity:   ${final_equity:,.2f}")
        print(f"Total Return:   {total_return_pct:.2f}%")
        print(f"Sharpe Ratio:   {sharpe_ratio:.2f}")
        print("-"*55)
        print(f"Total Trades:   {total_trades}")
        print(f"Win Rate:       {win_rate:.2f}%")
        print(f"Winning Trades: {len(winning_trades)}")
        print(f"Losing Trades:  {len(losing_trades)}")
        if total_trades > 0:
            avg_win = sum(t['pnl'] for t in winning_trades) / len(winning_trades) if winning_trades else 0
            avg_loss = sum(t['pnl'] for t in losing_trades) / len(losing_trades) if losing_trades else 0
            print(f"Avg Win:        ${avg_win:,.2f}")
            print(f"Avg Loss:       ${avg_loss:,.2f}")
            if avg_loss != 0:
                profit_factor = abs((avg_win * len(winning_trades)) / (avg_loss * len(losing_trades))) if len(losing_trades) > 0 else float('inf')
                print(f"Profit Factor:  {profit_factor:.2f}")
        print("="*55)

        pd.DataFrame(self.broker.equity_curve).to_csv(f"logs/equity_ai_{self.symbol}_simplified.csv", index=False)
        logger.info(f"Equity curve saved to logs/equity_ai_{self.symbol}_simplified.csv")

if __name__ == "__main__":
    pass