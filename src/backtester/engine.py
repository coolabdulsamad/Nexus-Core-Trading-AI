"""
src/backtester/engine.py  (v3.6)
Honest backtesting:
- MetaLearner runs in mode='backtest' -> memory search is time-filtered,
  no look-ahead, no future sentiment.
- Conviction gate |prob-0.5| works for BOTH longs and shorts.
- Signal QUALITY gates entry and drives position size tier.
- SL/TP detection uses bar high/low with fills at stop/limit prices.
- sma_cross exit disabled (structurally guaranteed-loss).
- v3: timeframe-aware - reads market_data{BAR_SUFFIX}/feature_cache{BAR_SUFFIX}
  and annualizes Sharpe with the correct bars-per-day for the timeframe.
- v3.2: entry rule matches the live trader - counter-trend signals pass with
  a quality penalty (x0.7) UNLESS the regime also opposes them; crypto is
  long-only. A per-bar decision funnel is printed.
- v3.6: FULL live/backtest parity. The v3.2 backtester had none of the live
  exit stack, which is why backtests looked better than live. Now ports:
  ENTRY: memory-scaled quality (q x min(1, n/80)), extreme-sentiment veto,
  toxic regime+sentiment combo veto, counter-regime entries must be STRONG,
  crypto momentum gate (price > sma200 AND 24h return > 0), session-open
  blackout for stocks, loss cooldowns (24h after a stop, 72h after 2 stops
  in 7d).
  EXITS: profit ratchet (replaces breakeven), hard trailing stop
  (activate +2.5 ATR / 2.5 ATR distance), retracement lock rebuilt
  (arm +2 ATR, keep 60% of peak), in-trade re-analysis (flip against in
  profit -> exit now; flip against underwater -> tighten stop once;
  otherwise 2-flip confirm), time limit 16 bars.
- v3.6.1 (post first-run fixes): the broker simulator's OWN hidden trailing
  stop (1.5/0.8 ATR) is now disabled (atr=None) - it was pre-empting the v3.6
  stack, the exact live/backtest divergence v3.6 was built to kill. Sentiment
  vetoes now read point-in-time feature_cache sentiment (the backtest reason
  string always reports sent=+0.00). Equity CSV path sanitized for crypto.
  KNOWN DIFFERENCE vs live: scale-outs (1/3 @ +1 ATR, 1/3 @ +2 ATR) and the
  time-based partial are live-only - the broker simulator closes whole
  positions. Omitting them is CONSERVATIVE for winners (backtest keeps full
  size longer) and neutral for losers.
"""
import re
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
BARS_PER_DAY = 390 / config.BAR_MINUTES          # 78 at 5-min, 6.5 at 1h
COUNTER_TREND_PENALTY = 0.7   # same rule as the live trader

# v3.6 exit-stack switches - hardcoded module constants in the live trader
# (live_paper_trader_multi.py lines 177-211), mirrored here for parity.
TRAILING_STOP_ENABLED = True
PROFIT_PROTECTION_ENABLED = True
SIGNAL_FLIP_EXIT_ENABLED = True
SIGNAL_FLIP_CONFIRM = 2
TIME_LIMIT_ENABLED = True

# v3.6.3: entry confirmation layer + data-calibrated gates (mirrors live).
# Measured over 39,570 live brain readings: max q EVER = 0.480, so the old
# 0.60 STRONG gates could never fire; recalibrated to 0.45 in settings.
ENTRY_BAR_CONFIRM = getattr(config, 'ENTRY_BAR_CONFIRM_ENABLED', True)
ENTRY_VWAP_CONFIRM = getattr(config, 'ENTRY_VWAP_CONFIRM_ENABLED', True)
ENTRY_NO_CHASE = getattr(config, 'ENTRY_NO_CHASE_ENABLED', True)
ENTRY_NO_CHASE_MAX_RANGE_ATR = getattr(config, 'ENTRY_NO_CHASE_MAX_RANGE_ATR', 1.5)
ENTRY_ADX_MIN = getattr(config, 'ENTRY_ADX_MIN', 20.0)
CRYPTO_MIN_SIGNAL_QUALITY = getattr(config, 'CRYPTO_MIN_SIGNAL_QUALITY', 0.25)


# ---------------------------------------------------------------------------
# Defensive accessors - BrokerSimulator position dict keys are not guaranteed
# ('sl' vs 'stop_loss', 'entry_price' vs 'entry').
# ---------------------------------------------------------------------------
def _pos_get(pos, *keys, default=None):
    for k in keys:
        if k in pos:
            return pos[k]
    return default


def _pos_sl(pos):
    return _pos_get(pos, 'sl', 'stop_loss')


def _pos_set_sl(pos, value, side):
    """Move the stop ONLY in the favorable direction (up for longs, down for shorts)."""
    cur = _pos_sl(pos)
    if cur is not None:
        value = max(cur, value) if side == 'LONG' else min(cur, value)
    if 'sl' in pos:
        pos['sl'] = value
    elif 'stop_loss' in pos:
        pos['stop_loss'] = value
    else:
        pos['sl'] = value


class BacktesterEngine:
    def __init__(self, symbol: str, initial_capital: float = 100000.0,
                 start_date: str = "2026-07-01", end_date: str = "2026-09-02"):
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.conn = psycopg2.connect(config.database.url)
        self.risk = RiskGate(initial_capital)
        self.broker = BrokerSimulator(initial_capital)
        self.meta_learner = MetaLearner()
        self.df = None
        self.cooldown = 0
        # v3.6: loss-cooldown memory (epoch seconds)
        self.last_stop_time = 0.0
        self.stop_history = []
        self.funnel = dict(bars=0, buy=0, sell=0, hold=0, thin_memory=0,
                           counter_trend_allowed=0, blocked_trend_opposed=0,
                           blocked_crypto_short=0, blocked_conviction=0,
                           blocked_volatility=0, blocked_quality=0,
                           blocked_daily_guard=0, blocked_size=0, entries=0,
                           blocked_session_open=0, blocked_sentiment=0,
                           blocked_toxic_combo=0, blocked_counter_regime=0,
                           blocked_crypto_momentum=0, blocked_loss_cooldown=0,
                           blocked_confirmation=0,
                           conf_bar_confirm=0, conf_vwap_confirm=0,
                           conf_no_chase=0, conf_adx=0)

    def fetch_data(self) -> pd.DataFrame:
        suffix = config.BAR_SUFFIX
        df = pd.read_sql(f"""
            SELECT m.time_bucket AS timestamp, m.open, m.high, m.low, m.close, m.volume, m.vwap,
                   f.rsi_14, f.macd_line, f.macd_signal, f.macd_hist,
                   f.bb_pct_b, f.bb_width, f.atr_14, f.atr_pct,
                   f.volume_profile_ratio, f.vol_z, f.ret_1, f.ret_3, f.ret_12,
                   f.adx_14, f.dist_sma50, f.dist_sma200, f.dist_vwap,
                   f.hour_sin, f.hour_cos, f.sentiment_score, f.regime_label
            FROM market_data{suffix} m
            JOIN feature_cache{suffix} f ON m.symbol = f.symbol AND m.time_bucket = f.time_bucket
            WHERE m.symbol = %s AND m.time_bucket BETWEEN %s AND %s
            ORDER BY m.time_bucket ASC
        """, self.conn, params=(self.symbol, self.start_date, self.end_date))
        if df.empty:
            logger.error(f"No data for {self.symbol} (tables *{suffix or ' (5-min)'})")
            return df

        df['sma_200'] = df['close'].rolling(200).mean()
        df['atr_50_avg'] = df['atr_14'].rolling(50).mean()
        df['volatility_ratio'] = df['atr_14'] / df['atr_50_avg']
        df = df.dropna(subset=['sma_200', 'atr_50_avg'])
        logger.info(f"Loaded {len(df)} candles for {self.symbol} ({config.BAR_MINUTES}-min bars)")
        return df

    # ------------------------------------------------------------------
    # v3.6 helpers (mirror the live trader)
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_reason(reason: str):
        """Pull regime / sentiment / memory depth out of the brain's reason string."""
        regime, sent, n = '', None, 0
        m = re.search(r'regime=(\w+)', reason or '')
        if m:
            regime = m.group(1)
        m = re.search(r'sent=([+-]?[\d\.]+)', reason or '')
        if m:
            sent = float(m.group(1))
        m = re.search(r'n=(\d+)', reason or '')
        if m:
            n = int(m.group(1))
        return regime, sent, n

    def _session_open_blackout(self, t) -> bool:
        """No stock entries in the first SESSION_OPEN_NO_ENTRY_MINUTES of the US session."""
        if '/' in self.symbol:
            return False
        mins = getattr(config, 'SESSION_OPEN_NO_ENTRY_MINUTES', 60)
        if mins <= 0:
            return False
        if t.weekday() >= 5:
            return False
        open_min = 13 * 60 + 30                     # 9:30 ET = 13:30 UTC (EDT)
        cur_min = t.hour * 60 + t.minute
        return open_min <= cur_min < open_min + mins

    def _crypto_momentum_ok(self, idx, price, sma) -> bool:
        """LONG only above the 200-bar average AND with a positive 24h return."""
        if not getattr(config, 'CRYPTO_MOMENTUM_GATE', True):
            return True
        if price <= sma:
            return False
        if idx >= 24:
            ret_24h = price / float(self.df.iloc[idx - 24]['close']) - 1.0
            return ret_24h > 0
        return False

    def _min_quality(self) -> float:
        """v3.6.3: crypto gets its own (lower) quality floor - mirrors live."""
        return CRYPTO_MIN_SIGNAL_QUALITY if '/' in self.symbol else config.MIN_SIGNAL_QUALITY

    def _entry_confirmation(self, row, side0: str, price: float, sma: float):
        """v3.6.3: the tape must agree with the brain's vote (mirrors live).
        The volatility filter is NOT repeated here - the engine already
        enforces it via vol_ok / blocked_volatility."""
        ret_1 = row.get('ret_1')
        atr_pct = row.get('atr_pct')
        dist_vwap = row.get('dist_vwap')
        adx = row.get('adx_14')

        if ENTRY_BAR_CONFIRM and ret_1 is not None and pd.notna(ret_1):
            if side0 == 'LONG' and ret_1 <= 0:
                return False, 'bar_confirm'
            if side0 == 'SHORT' and ret_1 >= 0:
                return False, 'bar_confirm'

        if ENTRY_VWAP_CONFIRM and dist_vwap is not None and pd.notna(dist_vwap):
            if side0 == 'LONG' and dist_vwap < 0:
                return False, 'vwap_confirm'
            if side0 == 'SHORT' and dist_vwap > 0:
                return False, 'vwap_confirm'

        if ENTRY_NO_CHASE and ret_1 is not None and atr_pct is not None \
                and pd.notna(ret_1) and pd.notna(atr_pct) and atr_pct > 0:
            if abs(ret_1) > ENTRY_NO_CHASE_MAX_RANGE_ATR * atr_pct:
                return False, 'no_chase'

        if ENTRY_ADX_MIN > 0 and adx is not None and pd.notna(adx):
            with_trend = ((side0 == 'LONG' and price > sma) or (side0 == 'SHORT' and price < sma))
            if with_trend and adx < ENTRY_ADX_MIN:
                return False, 'adx'

        return True, ''

    def _loss_cooldown_active(self, t) -> bool:
        """24h ban after a stop-out; 72h ban after 2 stops inside 7 days."""
        now = t.timestamp()
        window = getattr(config, 'REPEAT_LOSS_WINDOW_DAYS', 7) * 86400
        self.stop_history = [x for x in self.stop_history if now - x < window]
        if len(self.stop_history) >= 2:
            wait = getattr(config, 'REPEAT_LOSS_COOLDOWN_HOURS', 72) * 3600
            if now - self.stop_history[-1] < wait:
                return True
        wait = getattr(config, 'LOSS_COOLDOWN_HOURS', 24) * 3600
        if self.last_stop_time and now - self.last_stop_time < wait:
            return True
        return False

    def _record_new_closes(self, t):
        """Record realized pnl once per closed trade; feed stop-outs to the
        loss-cooldown memory."""
        if not self.broker.closed_trades:
            return
        last = self.broker.closed_trades[-1]
        if last.get('_counted') is True:
            return
        self.risk.record_realized_pnl(last['pnl'])
        last['_counted'] = True
        if 'stop' in str(last.get('reason', '')).lower():
            self.last_stop_time = t.timestamp()
            self.stop_history.append(t.timestamp())

    # ------------------------------------------------------------------
    def _sma_exit_confirmed(self, idx, direction) -> bool:
        """Buffered + confirmed SMA cross exit (disabled by config)."""
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
    def _manage_open_position(self, idx, row, t, price, atr, signal, quality):
        """v3.6 exit stack - mirrors live manage_exit(). Returns exit reason or None."""
        pos = self.broker.open_positions[0]
        side = pos['type']
        entry = _pos_get(pos, 'entry_price', 'entry', default=price)
        if atr is None or atr <= 0 or np.isnan(atr):
            return None

        if side == 'LONG':
            atr_profit = (price - entry) / atr
        else:
            atr_profit = (entry - price) / atr
        peak = max(pos.get('_v36_peak', 0.0), atr_profit)
        pos['_v36_peak'] = peak

        # 2) Profit ratchet (replaces the breakeven lock): at +PROFIT_RATCHET_ATR
        #    the stop jumps to entry +/- RATCHET_LOCK_ATR. Real money locked.
        if not pos.get('_v36_ratchet') and atr_profit >= config.PROFIT_RATCHET_ATR:
            lock = (entry + config.RATCHET_LOCK_ATR * atr) if side == 'LONG' \
                else (entry - config.RATCHET_LOCK_ATR * atr)
            _pos_set_sl(pos, lock, side)
            pos['_v36_ratchet'] = True

        # 4) Trailing stop (hard): activates at +TRAILING_STOP_ACTIVATE_ATR,
        #    trails TRAILING_STOP_DISTANCE_ATR behind price.
        if TRAILING_STOP_ENABLED and atr_profit >= config.TRAILING_STOP_ACTIVATE_ATR:
            trail = (price - config.TRAILING_STOP_DISTANCE_ATR * atr) if side == 'LONG' \
                else (price + config.TRAILING_STOP_DISTANCE_ATR * atr)
            _pos_set_sl(pos, trail, side)

        # 5) Retracement lock: arms only after a REAL peak (+RETRACEMENT_ARM_ATR)
        #    and keeps RETRACEMENT_KEEP_PCT of it.
        if PROFIT_PROTECTION_ENABLED and peak >= config.RETRACEMENT_ARM_ATR:
            if atr_profit <= peak * config.RETRACEMENT_KEEP_PCT:
                return 'retracement_lock'

        # 8) Signal flip + in-trade re-analysis
        if SIGNAL_FLIP_EXIT_ENABLED:
            opposite = 'SELL' if side == 'LONG' else 'BUY'
            if signal == opposite:
                if quality >= config.MIN_SIGNAL_QUALITY and atr_profit >= config.FLIP_EXIT_PROFIT_ATR:
                    return 'signal_flip_profit'
                if (getattr(config, 'FLIP_TIGHTEN_UNDERWATER', True)
                        and atr_profit < 0 and not pos.get('_v36_tightened')
                        and quality >= config.MIN_SIGNAL_QUALITY):
                    pos['_v36_tightened'] = True
                    tight = (price - 1.0 * atr) if side == 'LONG' else (price + 1.0 * atr)
                    _pos_set_sl(pos, tight, side)
                pos['_flip_count'] = pos.get('_flip_count', 0) + 1
                if pos['_flip_count'] >= SIGNAL_FLIP_CONFIRM:
                    return 'signal_flip'
            else:
                pos['_flip_count'] = 0

        # 9) SMA cross exit (disabled by config)
        if self._sma_exit_confirmed(idx, side):
            return 'sma_cross'

        # 10) Time limit
        bar_age = (t - pos['entry_time']).total_seconds() / BAR_SECONDS
        if TIME_LIMIT_ENABLED and bar_age >= config.TIME_LIMIT_BARS:
            return 'time_limit'
        return None

    # ------------------------------------------------------------------
    def run(self):
        self.df = self.fetch_data().reset_index(drop=True)
        if self.df.empty:
            return
        logger.info(f"Starting honest AI backtest for {self.symbol} ({config.BAR_MINUTES}-min)...")

        for idx, row in self.df.iterrows():
            price = row['close']
            t = row['timestamp']
            atr = row['atr_14']

            self.risk.reset_daily_if_new_day(t)
            equity = self.risk.capital = self.broker.get_total_equity(price)

            # 1. Cooldown
            if self.cooldown > 0:
                self.cooldown -= 1
                # v3.6.1: atr=None on PURPOSE - BrokerSimulator.check_positions
                # runs its OWN hidden trailing stop (1.5/0.8 ATR) when atr is
                # passed. Live has no such thing; the v3.6 engine stack owns
                # the stop. SL/TP detection does not need atr.
                self.broker.check_positions(t, row['high'], row['low'], price, None)
                self._record_new_closes(t)
                self._log_equity(t, price, 'HOLD', 0.5, 0.0)
                continue

            # 2. SL/TP (intrabar) - atr=None: broker's internal trailing
            # disabled, the v3.6 engine stack owns the stop (parity with live).
            self.broker.check_positions(t, row['high'], row['low'], price, None)
            self._record_new_closes(t)

            # 3. AI signal (point-in-time)
            result = self.meta_learner.get_signal(self.symbol, row, timestamp=t, mode='backtest')
            signal, prob = result['signal'], result['confidence']
            quality = result.get('quality', 0.0)
            reason = result.get('reason', '')

            # 3b. Decision funnel accounting (every bar is explained)
            self.funnel['bars'] += 1
            self.funnel[signal.lower()] += 1
            if 'thin_memory' in str(reason):
                self.funnel['thin_memory'] += 1

            # 4. Gates
            vol_ok = True
            if config.VOLATILITY_FILTER_ENABLED:
                vol_ok = row['volatility_ratio'] < config.VOLATILITY_RATIO_MAX
            conviction_ok = abs(prob - 0.5) >= config.ENTRY_CONVICTION_MARGIN
            can_trade, day_reason = self.risk.can_open_new_position(equity)

            # 5. Entry (flat only) - v3.6 gate chain, mirrors live check_entry
            if len(self.broker.open_positions) == 0:
                proceed = signal in ('BUY', 'SELL')
                eff_quality = quality
                if proceed:
                    side0 = 'LONG' if signal == 'BUY' else 'SHORT'
                    sma = row['sma_200']

                    # v3.6: quality is only as good as the memory behind it
                    r_regime, r_sent, n_mem = self._parse_reason(reason)
                    regime = r_regime or (row.get('regime_label')
                                          if isinstance(row.get('regime_label'), str)
                                          and row.get('regime_label') else 'unknown')
                    s = row.get('sentiment_score')
                    if s is not None and pd.notna(s):
                        sent = float(s)            # point-in-time cache (what the brain sees)
                    elif r_sent is not None:
                        sent = r_sent
                    else:
                        sent = 0.0
                    ref_n = getattr(config, 'QUALITY_MEMORY_REF_N', 80)
                    eff_quality = quality * min(1.0, (n_mem / ref_n) if ref_n else 1.0) \
                        if n_mem else quality * 0.5

                    veto_long = getattr(config, 'SENTIMENT_VETO_LONG', -0.60)
                    veto_short = getattr(config, 'SENTIMENT_VETO_SHORT', 0.60)
                    toxic_sent = getattr(config, 'TOXIC_REGIME_SENT', -0.30)
                    regime_min_q = getattr(config, 'TREND_REGIME_MIN_QUALITY', 0.45)

                    if self._session_open_blackout(t):
                        self.funnel['blocked_session_open'] += 1
                        proceed = False
                    elif self._loss_cooldown_active(t):
                        self.funnel['blocked_loss_cooldown'] += 1
                        proceed = False
                    elif side0 == 'LONG' and sent <= veto_long:
                        self.funnel['blocked_sentiment'] += 1
                        proceed = False
                    elif side0 == 'SHORT' and sent >= veto_short:
                        self.funnel['blocked_sentiment'] += 1
                        proceed = False
                    elif side0 == 'LONG' and regime == 'trend_down' and sent <= toxic_sent:
                        self.funnel['blocked_toxic_combo'] += 1
                        proceed = False
                    elif side0 == 'SHORT' and regime == 'trend_up' and sent >= -toxic_sent:
                        self.funnel['blocked_toxic_combo'] += 1
                        proceed = False
                    elif ((side0 == 'LONG' and regime == 'trend_down') or
                          (side0 == 'SHORT' and regime == 'trend_up')) and eff_quality < regime_min_q:
                        self.funnel['blocked_counter_regime'] += 1
                        proceed = False
                    elif side0 == 'LONG' and '/' in self.symbol and \
                            not self._crypto_momentum_ok(idx, price, sma):
                        self.funnel['blocked_crypto_momentum'] += 1
                        proceed = False

                    # v3.6.3: the tape itself must agree (bar / VWAP / no-chase / ADX)
                    if proceed:
                        ok, _why = self._entry_confirmation(row, side0, price, sma)
                        if not ok:
                            self.funnel['blocked_confirmation'] += 1
                            self.funnel[f'conf_{_why}'] += 1
                            proceed = False

                if proceed:
                    with_trend = ((signal == 'BUY' and price > row['sma_200']) or
                                  (signal == 'SELL' and price < row['sma_200']))
                    if not with_trend:
                        # Counter-trend: fighting BOTH timeframes stays vetoed
                        # (live: "fights both timeframes" block); otherwise
                        # allowed at reduced quality.
                        opposed = ((signal == 'SELL' and regime == 'trend_up') or
                                   (signal == 'BUY' and regime == 'trend_down'))
                        if opposed:
                            self.funnel['blocked_trend_opposed'] += 1
                            proceed = False
                        else:
                            adj = eff_quality * COUNTER_TREND_PENALTY
                            if adj >= self._min_quality():
                                self.funnel['counter_trend_allowed'] += 1
                                eff_quality = adj
                            else:
                                self.funnel['blocked_quality'] += 1
                                proceed = False
                    if proceed and signal == 'SELL' and '/' in self.symbol:
                        self.funnel['blocked_crypto_short'] += 1
                        proceed = False
                if proceed:
                    if not conviction_ok:
                        self.funnel['blocked_conviction'] += 1
                    elif not vol_ok:
                        self.funnel['blocked_volatility'] += 1
                    elif eff_quality < self._min_quality():
                        self.funnel['blocked_quality'] += 1
                    elif not can_trade:
                        self.funnel['blocked_daily_guard'] += 1
                    else:
                        side = 'LONG' if signal == 'BUY' else 'SHORT'
                        size, sl, tp = self.risk.size_with_tier(price, atr, eff_quality, side)[:3]
                        if size > 0:
                            if side == 'LONG':
                                self.broker.open_long(t, self.risk.apply_slippage(price, True), size, sl, tp)
                            else:
                                self.broker.open_short(t, self.risk.apply_slippage(price, True), size, sl, tp)
                            self.funnel['entries'] += 1
                            logger.info(f"{signal} ({reason}) q={quality:.3f} eff={eff_quality:.3f}")
                        else:
                            self.funnel['blocked_size'] += 1
            else:
                # 6. Exit management (v3.6 stack)
                exit_reason = self._manage_open_position(idx, row, t, price, atr, signal, quality)

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
        sharpe = (rets.mean() / rets.std() * np.sqrt(BARS_PER_DAY * 252)) if len(rets) > 1 and rets.std() != 0 else 0
        max_dd = ((eq.cummax() - eq) / eq.cummax()).max() * 100

        trades = self.broker.closed_trades
        wins = [t for t in trades if t['pnl'] > 0]
        losses = [t for t in trades if t['pnl'] <= 0]
        win_rate = len(wins) / len(trades) * 100 if trades else 0

        print("\n" + "=" * 60)
        print(f"HONEST AI BACKTEST v3.6: {self.symbol} ({config.BAR_MINUTES}-min bars)")
        print("=" * 60)
        print(f"Period:         {self.start_date} -> {self.end_date}")
        print(f"Initial:        ${init:,.2f}   Final: ${final:,.2f}   Return: {ret_pct:+.2f}%")
        print(f"Sharpe:         {sharpe:.2f}   Max Drawdown: {max_dd:.2f}%")
        print("-" * 60)
        print(f"Trades: {len(trades)}   Win rate: {win_rate:.1f}%   W/L: {len(wins)}/{len(losses)}")
        print("-" * 60)
        print("DECISION FUNNEL (why bars did/didn't trade):")
        f = self.funnel
        print(f"  Bars evaluated:         {f['bars']:<6} BUY={f['buy']} SELL={f['sell']} HOLD={f['hold']}")
        print(f"  Memory thin/missing:    {f['thin_memory']:<6} (if this equals bars, Qdrant/memory is DOWN)")
        print(f"  Entries taken:          {f['entries']:<6} (counter-trend allowed: {f['counter_trend_allowed']})")
        print(f"  Blocked by quality:     {f['blocked_quality']:<6} (incl. counter-trend x{COUNTER_TREND_PENALTY} penalty)")
        print(f"  Blocked sentiment veto: {f['blocked_sentiment']:<6} Blocked toxic combo: {f['blocked_toxic_combo']}")
        print(f"  Blocked counter-regime: {f['blocked_counter_regime']:<6} (needs eff_q >= {getattr(config, 'TREND_REGIME_MIN_QUALITY', 0.45):.2f})")
        print(f"  Blocked crypto momentum:{f['blocked_crypto_momentum']:<6} Blocked session-open: {f['blocked_session_open']}")
        print(f"  Blocked loss-cooldown:  {f['blocked_loss_cooldown']:<6} Blocked trend-opposed: {f['blocked_trend_opposed']}")
        print(f"  Blocked conviction:     {f['blocked_conviction']:<6} Blocked volatility: {f['blocked_volatility']}")
        print(f"  Blocked confirmation:   {f['blocked_confirmation']:<6} (v3.6.3: bar/VWAP/no-chase/ADX)")
        print(f"    -> bar: {f['conf_bar_confirm']:<5} vwap: {f['conf_vwap_confirm']:<5} no-chase: {f['conf_no_chase']:<5} adx: {f['conf_adx']}")
        print(f"  Blocked daily-guard:    {f['blocked_daily_guard']:<6} Blocked size: {f['blocked_size']}  Crypto shorts skipped: {f['blocked_crypto_short']}")
        if trades:
            avg_w = np.mean([t['pnl'] for t in wins]) if wins else 0
            avg_l = np.mean([t['pnl'] for t in losses]) if losses else 0
            pf = abs(sum(t['pnl'] for t in wins) / sum(t['pnl'] for t in losses)) if losses and sum(t['pnl'] for t in losses) != 0 else float('inf')
            print(f"Avg win: ${avg_w:,.2f}   Avg loss: ${avg_l:,.2f}   Profit factor: {pf:.2f}")
            print("-" * 60)
            print("EXIT REASON BREAKDOWN:")
            reasons = {}
            for t in trades:
                r = reasons.setdefault(t['reason'], {'n': 0, 'pnl': 0.0, 'wins': 0})
                r['n'] += 1
                r['pnl'] += t['pnl']
                r['wins'] += 1 if t['pnl'] > 0 else 0
            for reason, r in sorted(reasons.items(), key=lambda x: x[1]['pnl']):
                print(f"  {reason:<20} n={r['n']:<4} win%={r['wins']/r['n']*100:5.1f}  pnl=${r['pnl']:+,.2f}")
        print("=" * 60)

        import os
        os.makedirs('logs', exist_ok=True)
        safe_symbol = self.symbol.replace('/', '_')
        pd.DataFrame(self.broker.equity_curve).to_csv(f"logs/equity_ai_{safe_symbol}.csv", index=False)


if __name__ == "__main__":
    import sys
    symbol = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    start = sys.argv[2] if len(sys.argv) > 2 else "2026-07-01"
    end = sys.argv[3] if len(sys.argv) > 3 else "2026-09-02"
    BacktesterEngine(symbol, start_date=start, end_date=end).run()
