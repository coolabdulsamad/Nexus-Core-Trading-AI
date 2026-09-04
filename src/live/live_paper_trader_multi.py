#!/usr/bin/env python3
"""
src/live/live_paper_trader_multi.py  (v3 -> 1h)
================================================================
Multi-symbol live paper trading on Alpaca with the v3 1-hour brain.

Pipeline per cycle per symbol:
  Alpaca latest 1h bar -> indicators v2 -> MetaLearner -> quality tier
  -> ATR-based bracket (paper) -> profit-lock exit stack -> Telegram.

- Universe from DB (migration 003) + daily top-N selector; falls back to
  config lists.
- Stocks trade only when the market is open; crypto trades 24/7.
- 1h bars: same brain/memory as the honest backtester (market_memory_60m).

v3.0.1 fixes:
- get_bars: Alpaca returns bars ASCENDING from `start`, so a small API
  limit returns the OLDEST bars (crypto showed 660h-old data, and
  get_latest_price fetched 40-day-old prices). Now fetches the full
  window server-side and tails client-side.
- _send_eod / _send_heartbeat: corrected to reporter.py's actual
  signatures (trade dict list + day_start/peak equity).

v3.2 (the "zero trades" post-mortem):
- enter_position/manage_exit computed the v2 indicator set on a 60-bar
  window; calculate_all_indicators() dropna()s the 200-bar warm-up, so the
  frame came back EMPTY and every entry (plus all ATR-based exit logic,
  including stop-loss checks) silently aborted. Both now use the full bar
  window. This bug blocked 100% of entries since v3 launched.
- manage_exit: hard SL/TP now checked BEFORE any indicator dependency, so
  positions stay protected even during data hiccups.
- Trend filter: hard 200-SMA veto -> regime-aware. Counter-trend signals
  pass with a quality penalty (x0.7) when the short-term regime does not
  oppose them; only fighting BOTH timeframes stays vetoed.
- Crypto is long-only (Alpaca spot has no shorting): crypto SELL signals
  are logged and skipped instead of attempting impossible orders.
- Every bail in the entry path now logs its reason.

v3.4 (position adoption + honest sizing):
- The trader reconciles with Alpaca at startup AND every cycle: positions
  opened before a restart/crash (or manually at the broker) are ADOPTED -
  bracket rebuilt from the broker's avg entry + current ATR - and managed
  with the exact same exit rules as new trades. Restarts no longer orphan
  open positions, and positions closed while the bot was down are cleared.
  The reconcile itself never closes anything; only the normal exit stack
  can close.
- Sizing now uses REAL account equity (slice = equity x SLICE_PCT_OF_EQUITY,
  capped at CAPITAL_PER_SYMBOL) and every entry is capped by live buying
  power. The old fixed $100k/symbol slice could deploy ~3x the account,
  which is how the paper account ended up at -$206k cash.

v3.5 (single-instance lock + close-race guard + crypto cash fix):
- A file lock (logs/.trader.lock) makes a second concurrent trader process
  REFUSE to start. Two instances on one account were fighting each other:
  one's close became the other's new short, positions doubled round after
  round (INTC 52 -> 4032 shares) and closes started failing with
  "insufficient buying power" (Alpaca treats a sell beyond the held qty as
  opening a short, which needs margin).
- close_position re-reads the live broker qty immediately before selling
  and caps the close at what is actually held, so a close can never
  overshoot into a short even in a race.
- Crypto entries are now capped by non_marginable_buying_power (settled
  USD), not the margin buying_power field - Alpaca crypto is
  non-marginable, so the old cap let unfillable orders through and spammed
  "insufficient balance for USD" every cycle.

v3.5.1: the per-cycle reconcile now also detects a SIDE FLIP (broker holds
the opposite side of what we track - e.g. a leftover short from the
two-process fight while we adopted a long) and re-adopts from the broker's
real side instead of managing it with a backwards bracket.

v3.5.2 (the PLTR pile-up post-mortem, Aug 26):
- close_position now derives the close direction from the BROKER's live
  signed qty, not from tracked state. Tracked state was briefly wrong
  during the pile-up, which turned "closes" into orders in the wrong
  direction; that class of bug is now impossible by construction.
- Close fallback: if a market-order close is rejected twice (e.g.
  "insufficient buying power" when Alpaca sees the shares tied up by a
  foreign order), cancel resting orders and use Alpaca's server-side
  position liquidation, which is side- and qty-agnostic. 3+ failures send
  an urgent Telegram instead of retrying silently for hours.
- Reconcile now checks SIZE as well as side: if the broker qty changed
  without any order from this process, it re-syncs to the broker size and
  raises a loud "second trader" alert. The old side-only check watched
  PLTR double 55 -> 110 -> 330 -> 660 -> 1320 and saw nothing wrong.
- Runaway-size circuit breaker: a position bigger than 2x its sizing
  slice cannot have come from this strategy; the bot alerts and refuses
  to add more.
- Partial closes (partial TP / time partial) now re-sync the tracked qty
  from the broker right after filling, so later size checks stay honest.
- Entry: post-fill verification checks the resulting SIDE as well as the
  qty; a rejected entry with "insufficient" now backs off for 30 min
  instead of spamming a failing order every 5 minutes.
- The stale-data warning now names the symbol.

v3.6 (the "no edge" post-mortem, Sep 2 - one week of evidence):
- ENTRY ANALYSIS GATES. The losing cluster was identical every time: BUY in
  regime=trend_down with fearful sentiment on a thin (n=35) memory, quality
  ~0.45 that predicted nothing (winners and losers both averaged q~0.40).
  Now: quality is scaled by memory depth (q x min(1, n/80)) and the floor is
  0.35; extreme sentiment vetoes entries; trend_down longs must be STRONG;
  crypto longs need price > 200-bar SMA AND a positive 24h return (crypto
  sentiment is always 0 - no news feed - so momentum is the honesty check);
  no stock entries in the first 60 min of the session.
- EXIT STACK REBUILD. The old retracement lock armed at +0.7 ATR and exited
  at +0.5 ATR: winners were cut to ~+$28 crumbs (median hold 1.6h) while
  losers ran the full -2 ATR stop (avg -$260). Breakeven and trailing fired
  ZERO times in a week. Now: scale out 1/3 at +1 ATR and +2 ATR; the stop
  ratchets to entry+0.5 ATR at +1.5 ATR profit; a real trailing stop from
  +2.5 ATR; the retracement lock arms at +2 ATR and keeps 60% of the peak.
- IN-TRADE RE-ANALYSIS. Every cycle the brain re-judges the open position:
  flipped against while up >= 0.5 ATR -> take profit immediately; flipped
  against while underwater -> tighten the stop to 1 ATR (once).
- LOSS COOLDOWNS. A stop-out bans the symbol for 24h (two stops in a week:
  72h). ETH was stopped 3x in 7h on Aug 30-31 on a 5-minute cooldown.
- Telegram: entries show the full "why" (q, n, regime, sentiment, vetoes);
  ratchets, scale-outs and flips against open positions alert immediately;
  exits report R-multiple, hold time and % of peak kept.

v3.6.2 (observability): position heartbeat card every 4 bars, trailing-stop
move alerts, retracement-arm alert, per-cycle REANALYSIS log lines.

v3.6.3 (data-calibrated gates + entry confirmation):
- Measured over 39,570 live brain readings: max q EVER observed = 0.480, so
  the 0.60 STRONG gates could never fire. Recalibrated: counter-regime STRONG
  and the STRONG sizing tier now 0.45 (~p95 of observed). Crypto gets its own
  quality floor (0.25) - its q distribution is structurally lower.
- ENTRY CONFIRMATION LAYER: the brain votes from memory, then the current
  tape must agree before money moves: last closed bar with the signal, price
  on the right side of VWAP, no chasing spike bars (>1.5x ATR), with-trend
  entries need ADX >= 20, and the volatility filter (ATR spike vs its own
  average) is now actually enforced (it existed in config but was never wired).
"""
import os
import sys
import time
import logging
import re
import subprocess
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

try:
    import fcntl
except ImportError:
    fcntl = None   # non-Linux: single-instance lock disabled (warned at startup)

from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
load_dotenv()

from config.settings import config
from src.models.meta_learner import MetaLearner
from src.ingestion.indicator_calculator import calculate_all_indicators
from src.utils.telegram import send_telegram
from src.utils.reporter import send_eod_report, format_capital_update
from src.utils.logger import setup_logger

logger = setup_logger("LivePaperTrader", "logs/live_trader.log")

# ----- API -----
API_KEY = os.getenv("ALPACA_API_KEY")
API_SECRET = os.getenv("ALPACA_SECRET_KEY")
PAPER = os.getenv("ALPACA_PAPER", "true").lower() == "true"

# ----- Core params -----
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))   # seconds between cycles
MAX_POSITIONS = 3
CAPITAL_PER_SYMBOL = 100000.0

# ----- Trade structure (1h bars) -----
STOP_ATR_MULT = config.STOP_ATR_MULT               # 2.0
REWARD_RISK_RATIO = config.REWARD_RISK_RATIO       # 1.5
PARTIAL_TP_THRESHOLD = config.PARTIAL_TP_THRESHOLD
PARTIAL_CLOSE_PCT = config.PARTIAL_CLOSE_PCT
TRAILING_TP_ACTIVATE = config.TRAILING_TP_ATR_TRIGGER
TRAILING_TP_DISTANCE = config.TRAILING_TP_DISTANCE_ATR
BREAKEVEN_ATR = config.BREAKEVEN_ATR_MULTIPLE      # 1.5

TRAILING_STOP_ACTIVATE = config.TRAILING_STOP_ACTIVATE_ATR   # v3.6: 2.5 ATR (was hardcoded 4.0 - never fired)
TRAILING_STOP_DISTANCE = config.TRAILING_STOP_DISTANCE_ATR   # v3.6: 2.5 ATR (was hardcoded 6.0)
TRAILING_STOP_ENABLED = True

MAX_SPREAD_PCT = 0.002

# ----- Counter-trend entries (v3.2) -----
# The brain is mean-reverting: at highs it often says SELL while price is
# still above the 200-bar average. A hard veto there blocked every entry
# for weeks. Counter-trend signals now pass at reduced quality UNLESS the
# short-term regime also disagrees (fighting both timeframes stays banned).
COUNTER_TREND_PENALTY = 0.7

ATR_SMA_PERIOD = 50

SMA_PERIOD = 200                  # 200 1h bars ~= 1 month
SMA_EXIT_ENABLED = config.SMA_EXIT_ENABLED
SMA_EXIT_CONFIRM_BARS = config.SMA_EXIT_CONFIRM_BARS

PROFIT_PROTECTION_ENABLED = True
RETRACEMENT_ARM_ATR = config.RETRACEMENT_ARM_ATR        # v3.6: arm at +2 ATR peak (was 0.70)
RETRACEMENT_KEEP_PCT = config.RETRACEMENT_KEEP_PCT      # v3.6: keep 60% of peak (was fixed 0.50 ATR)
PROFIT_RATCHET_ATR = config.PROFIT_RATCHET_ATR
RATCHET_LOCK_ATR = config.RATCHET_LOCK_ATR

DAILY_PROFIT_TARGET_ENABLED = config.DAILY_PROFIT_TARGET_PCT > 0
DAILY_TARGET_PCT = config.DAILY_PROFIT_TARGET_PCT
LOCK_BREAKEVEN_ON_TARGET = config.DAILY_TARGET_LOCK_BREAKEVEN

SIGNAL_FLIP_EXIT_ENABLED = True
SIGNAL_FLIP_CONFIRM = 2

TIME_PARTIAL_ENABLED = config.ENABLE_TIME_PARTIAL
TIME_PARTIAL_BARS = 12            # 12 hours
TIME_PARTIAL_PROFIT_ATR = config.TIME_PARTIAL_PROFIT_ATR

TIME_LIMIT_ENABLED = True
TIME_LIMIT_BARS = config.TIME_LIMIT_BARS   # 16 hours (v3.6)

# v3.6.2 observability: position heartbeats + trailing-move alerts
POSITION_HEARTBEAT_BARS = getattr(config, 'POSITION_HEARTBEAT_BARS', 4)
TRAIL_ALERT_STEP_ATR = getattr(config, 'TRAIL_ALERT_STEP_ATR', 0.25)

# v3.6.3: entry confirmation layer + data-calibrated gates.
# Measured over 39,570 live brain readings: max q EVER seen = 0.480, so the
# old 0.60 "STRONG" gates could never fire (0 trades). Recalibrated to 0.45
# (~p95 of observed). Crypto quality runs structurally lower (p90 = 0.21),
# so crypto gets its own floor - the momentum gate + these confirmations do
# the protective work. The confirmation layer makes the CURRENT tape agree
# with the brain's vote before money moves.
ENTRY_BAR_CONFIRM = getattr(config, 'ENTRY_BAR_CONFIRM_ENABLED', True)
ENTRY_VWAP_CONFIRM = getattr(config, 'ENTRY_VWAP_CONFIRM_ENABLED', True)
ENTRY_NO_CHASE = getattr(config, 'ENTRY_NO_CHASE_ENABLED', True)
ENTRY_NO_CHASE_MAX_RANGE_ATR = getattr(config, 'ENTRY_NO_CHASE_MAX_RANGE_ATR', 1.5)
ENTRY_ADX_MIN = getattr(config, 'ENTRY_ADX_MIN', 20.0)
CRYPTO_MIN_SIGNAL_QUALITY = getattr(config, 'CRYPTO_MIN_SIGNAL_QUALITY', 0.25)
VOL_FILTER_ENABLED = getattr(config, 'VOLATILITY_FILTER_ENABLED', True)
VOL_RATIO_MAX = getattr(config, 'VOLATILITY_RATIO_MAX', 3.0)
COOLDOWN_BARS = config.COOLDOWN_BARS

MAX_DATA_AGE_SECONDS = 7200       # 1h bars: accept up to 2h old (hourly cadence)

TRADER_VERSION = "v3.6.3"

# Ghost-trader / runaway detection (v3.5.2)
SIZE_DRIFT_TOLERANCE = 0.02       # >2% qty change w/o our order => foreign trade
RUNAWAY_NOTIONAL_MULT = 2.0       # position > 2x its sizing slice => impossible from us
RUNAWAY_ALERT_SECONDS = 3600      # throttle runaway/ghost alerts per symbol
ENTRY_FAIL_COOLDOWN_CYCLES = 6    # 30 min backoff after an "insufficient" rejection

EOD_REPORT_ENABLED = config.TELEGRAM_EOD_REPORT
HEARTBEAT_CYCLES = config.TELEGRAM_HEARTBEAT_CYCLES

# Daily self-maintenance (v3.3): the trader itself launches the data pump +
# Qdrant outcome sync once a day in the background, so the memory can never
# silently go blind again (the Aug-14 Qdrant crash wiped forward_return_4h
# from every point -> thin_memory(0) -> a blind brain standing down).
DAILY_MAINTENANCE_ENABLED = getattr(config, 'DAILY_MAINTENANCE_ENABLED', True)
DAILY_MAINTENANCE_UTC_HOUR = getattr(config, 'DAILY_MAINTENANCE_UTC_HOUR', 7)   # 08:00 WAT

# Position adoption + honest sizing (v3.4)
ADOPT_BROKER_POSITIONS = getattr(config, 'ADOPT_BROKER_POSITIONS', True)
ADOPTED_TIME_LIMIT = getattr(config, 'ADOPTED_TIME_LIMIT_ENABLED', True)
USE_REAL_ACCOUNT_SIZING = getattr(config, 'USE_REAL_ACCOUNT_SIZING', True)
SLICE_PCT_OF_EQUITY = getattr(config, 'SLICE_PCT_OF_EQUITY', 0.33)
BUYING_POWER_USAGE_CAP = getattr(config, 'BUYING_POWER_USAGE_CAP', 0.95)
ADOPT_FALLBACK_SL_PCT = 0.05           # emergency bracket if ATR is unavailable at adoption

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class MultiSymbolPaperTrader:
    def __init__(self):
        # Single-instance lock (v3.5): two trader processes on one account
        # trade AGAINST each other (one's close becomes the other's short).
        # Refuse to start if another instance already holds the lock.
        if fcntl is not None:
            self._lock_fh = open(os.path.join(PROJECT_ROOT, "logs", ".trader.lock"), "a")
            try:
                fcntl.flock(self._lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                logger.error("Another trader instance is already running - refusing to start")
                send_telegram("Trader NOT started: another instance is already running. "
                              "Kill all trader processes first (tmux kill-server).", 'warning')
                sys.exit(1)
        else:
            logger.warning("fcntl unavailable - single-instance lock disabled")

        self.trading_client = TradingClient(API_KEY, API_SECRET, paper=PAPER)
        self.stock_data_client = StockHistoricalDataClient(API_KEY, API_SECRET)
        self.crypto_data_client = CryptoHistoricalDataClient(API_KEY, API_SECRET)
        self.meta_learner = MetaLearner()
        self._selector = None
        self._last_selection_date = None
        self._peak_equity = 0.0

        # Universe (DB + selector, config fallback)
        self.symbols, self.crypto_symbols = self._resolve_universe()
        self.all_symbols = self.symbols + self.crypto_symbols

        # Per-symbol state
        self.in_position = {s: False for s in self.all_symbols}
        self.position_side = {s: None for s in self.all_symbols}
        self.entry_price = {s: 0.0 for s in self.all_symbols}
        self.position_qty = {s: 0.0 for s in self.all_symbols}
        self.highest_price = {s: 0.0 for s in self.all_symbols}
        self.lowest_price = {s: float('inf') for s in self.all_symbols}
        self.breakeven_set = {s: False for s in self.all_symbols}
        self.trailing_tp_set = {s: False for s in self.all_symbols}
        self.partial_closed = {s: False for s in self.all_symbols}
        self.entry_bar_time = {s: None for s in self.all_symbols}
        self.last_flip_signal = {s: None for s in self.all_symbols}
        self.flip_count = {s: 0 for s in self.all_symbols}
        self.cooldown = {s: 0 for s in self.all_symbols}
        self.stop_loss = {s: None for s in self.all_symbols}
        self.take_profit = {s: None for s in self.all_symbols}
        self.daily_trades = {s: [] for s in self.all_symbols}
        self.daily_start_equity = {s: None for s in self.all_symbols}
        self.daily_realized_pnl = {s: 0.0 for s in self.all_symbols}
        self.daily_target_hit = {s: False for s in self.all_symbols}
        self.time_partial_done = {s: False for s in self.all_symbols}
        self.scale_out_1_done = {s: False for s in self.all_symbols}
        self.scale_out_2_done = {s: False for s in self.all_symbols}
        self.ratchet_done = {s: False for s in self.all_symbols}
        self.flip_tightened = {s: False for s in self.all_symbols}
        self.active_orders: Dict[str, str] = {}
        self.close_fail_count = {s: 0 for s in self.all_symbols}
        self.last_order_error: Dict[str, str] = {}
        self._ghost_alerted: Dict[str, float] = {}
        self._runaway_alerted: Dict[str, float] = {}
        self._last_equity = 0.0
        # v3.6: loss cooldowns (stop-out bans)
        self.last_stop_time: Dict[str, float] = {}
        self.stop_history: Dict[str, list] = {}
        # v3.6.2 observability state
        self._last_pos_heartbeat: Dict[str, int] = {}   # last heartbeat bar-number per symbol
        self._last_trail_alert: Dict[str, float] = {}   # last alerted trailing-stop price
        self.retracement_armed: Dict[str, bool] = {}    # retracement-lock arm alert sent
        self._last_brain: Dict[str, tuple] = {}         # brain's latest read (signal, q)

        self.last_report_date = None
        self.cycle_count = 0

        # Daily self-maintenance scheduler (pump + Qdrant outcome sync)
        self._maintenance_proc = None
        self._next_maintenance = self._initial_maintenance_time()
        if DAILY_MAINTENANCE_ENABLED and self._next_maintenance is not None:
            logger.info(f"Daily maintenance: next run at {self._next_maintenance.isoformat()}")

        # Adopt any positions the broker already holds (opened before this
        # process started - restart, crash, or manual). They are managed
        # with the same exit rules as new trades; nothing is closed here.
        if ADOPT_BROKER_POSITIONS:
            self._reconcile_positions()

    # ================= Universe =================
    @staticmethod
    def _is_crypto(symbol: str) -> bool:
        return '/' in symbol

    @staticmethod
    def _pos_key(symbol: str) -> str:
        return symbol.replace('/', '')

    def _resolve_universe(self) -> Tuple[List[str], List[str]]:
        try:
            from src.universe.symbol_manager import SymbolManager
            mgr = SymbolManager()
            if config.UNIVERSE_MODE == 'auto':
                from src.universe.selector import DailySelector
                self._selector = DailySelector()
                picks = self._selector.select(top_n=config.TOP_N_SYMBOLS)
                stocks = [p['symbol'] for p in picks if p['asset_type'] == 'stock']
                crypto = [p['symbol'] for p in picks if p['asset_type'] == 'crypto']
                logger.info(f"Universe (auto top-{config.TOP_N_SYMBOLS}): {stocks} + {crypto}")
                return stocks, crypto
            rows = mgr.list_symbols(active_only=True)
            stocks = [r['symbol'] for r in rows if r['asset_type'] == 'stock']
            crypto = [r['symbol'] for r in rows if r['asset_type'] == 'crypto']
            if stocks or crypto:
                logger.info(f"Universe (DB): {stocks} + {crypto}")
                return stocks, crypto
        except Exception as e:
            logger.warning(f"DB universe unavailable ({e}) -> config fallback")
        return list(config.symbols), (list(config.CRYPTO_SYMBOLS) if config.CRYPTO_ENABLED else [])

    def _refresh_universe_if_needed(self):
        if config.UNIVERSE_MODE != 'auto' or self._selector is None:
            return
        today = datetime.now(timezone.utc).date()
        if self._last_selection_date == today:
            return
        try:
            picks = self._selector.select(top_n=config.TOP_N_SYMBOLS)
            new_stocks = [p['symbol'] for p in picks if p['asset_type'] == 'stock']
            new_crypto = [p['symbol'] for p in picks if p['asset_type'] == 'crypto']
            for s in self.all_symbols:
                if self.in_position.get(s) and s not in new_stocks + new_crypto:
                    (new_stocks if not self._is_crypto(s) else new_crypto).append(s)
            for s in new_stocks + new_crypto:
                if s not in self.all_symbols:
                    self._init_symbol_state(s)
            self.symbols, self.crypto_symbols = new_stocks, new_crypto
            self.all_symbols = self.symbols + self.crypto_symbols
            self._last_selection_date = today
            logger.info(f"Universe refreshed: {self.all_symbols}")
        except Exception as e:
            logger.error(f"Universe refresh failed: {e}")

    def _init_symbol_state(self, s: str):
        self.in_position[s] = False
        self.position_side[s] = None
        self.entry_price[s] = 0.0
        self.position_qty[s] = 0.0
        self.highest_price[s] = 0.0
        self.lowest_price[s] = float('inf')
        self.breakeven_set[s] = False
        self.trailing_tp_set[s] = False
        self.partial_closed[s] = False
        self.time_partial_done[s] = False
        self.entry_bar_time[s] = None
        self.last_flip_signal[s] = None
        self.flip_count[s] = 0
        self.cooldown[s] = 0
        self.stop_loss[s] = None
        self.take_profit[s] = None
        self.daily_trades[s] = []
        self.daily_start_equity[s] = None
        self.daily_realized_pnl[s] = 0.0
        self.daily_target_hit[s] = False
        self.close_fail_count[s] = 0
        self.scale_out_1_done[s] = False
        self.scale_out_2_done[s] = False
        self.ratchet_done[s] = False
        self.flip_tightened[s] = False
        self._last_pos_heartbeat[s] = -1
        self._last_trail_alert[s] = None
        self.retracement_armed[s] = False
        self._last_brain[s] = None

    # ================= Market data =================
    def _get_data_client(self, symbol: str):
        return self.crypto_data_client if self._is_crypto(symbol) else self.stock_data_client

    def get_bars(self, symbol: str, limit: int = 300) -> Optional[pd.DataFrame]:
        """Latest `limit` CLOSED+forming 1h bars (most recent last)."""
        try:
            request_cls = CryptoBarsRequest if self._is_crypto(symbol) else StockBarsRequest
            req = request_cls(
                symbol_or_symbols=[symbol],
                timeframe=TimeFrame.Hour,
                start=datetime.now(timezone.utc) - timedelta(days=40),
                limit=10000,   # fetch the whole window; Alpaca returns ASC from start
            )
            client = self._get_data_client(symbol)
            bars = (client.get_crypto_bars(req) if self._is_crypto(symbol)
                    else client.get_stock_bars(req))
            df = bars.df
            if df.empty or symbol not in df.index.get_level_values(0):
                return None
            df = df.xs(symbol).reset_index()
            df = df.sort_values("timestamp").tail(limit).reset_index(drop=True)
            return df
        except Exception as e:
            logger.error(f"{symbol} bar fetch failed: {e}")
            return None

    def get_latest_price(self, symbol: str) -> Optional[float]:
        df = self.get_bars(symbol, limit=5)
        if df is None or df.empty:
            return None
        return float(df.iloc[-1]['close'])

    def compute_indicators(self, df: pd.DataFrame, symbol: str = '?') -> Optional[pd.Series]:
        """Latest CLOSED bar with the full v2 indicator set (same as backtester)."""
        try:
            if len(df) < SMA_PERIOD + 5:
                return None
            feats = calculate_all_indicators(
                df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy())
            if feats.empty:
                return None
            latest = feats.iloc[-1].copy()
            latest['close'] = df.iloc[-1]['close']
            latest['sma_200'] = df['close'].rolling(SMA_PERIOD).mean().iloc[-1]
            atr_sma = feats['atr_14'].rolling(ATR_SMA_PERIOD).mean().iloc[-1]
            latest['atr_50_avg'] = atr_sma
            latest['volatility_ratio'] = (latest['atr_14'] / atr_sma) if atr_sma and not np.isnan(atr_sma) else 1.0
            # if the last bar is still forming, use the previous CLOSED one
            if pd.Timestamp(df.iloc[-1]['timestamp']) > pd.Timestamp.now(tz='UTC') - pd.Timedelta(hours=1):
                latest = feats.iloc[-2].copy()
                latest['close'] = df.iloc[-2]['close']
                latest['sma_200'] = df['close'].rolling(SMA_PERIOD).mean().iloc[-2]
            bar_age = (pd.Timestamp.now(tz='UTC') - pd.Timestamp(latest['timestamp'])).total_seconds()
            if bar_age > MAX_DATA_AGE_SECONDS:
                logger.warning(f"{symbol}: stale data ({bar_age/3600:.1f}h old), skipping cycle")
                return None
            return latest
        except Exception as e:
            logger.error(f"Indicator computation failed: {e}")
            return None

    # ================= Account / positions =================
    def get_account(self):
        return self.trading_client.get_account()

    def _positions_map(self) -> Dict[str, float]:
        out = {}
        for p in self.trading_client.get_all_positions():
            out[p.symbol] = float(p.qty) if p.side.value == 'long' else -float(p.qty)
        return out

    def get_position_qty(self, symbol: str) -> float:
        return self._positions_map().get(self._pos_key(symbol), 0.0)

    # ================= Orders =================
    def _order_tif(self, symbol: str) -> TimeInForce:
        return TimeInForce.GTC if self._is_crypto(symbol) else TimeInForce.DAY

    def cancel_symbol_orders(self, symbol: str):
        try:
            for o in self.trading_client.get_orders():
                if o.symbol == self._pos_key(symbol) and o.status in (OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED):
                    self.trading_client.cancel_order_by_id(o.id)
        except Exception as e:
            logger.error(f"{symbol} cancel orders failed: {e}")

    def _wait_fill(self, order_id: str, timeout: int = None) -> bool:
        timeout = timeout or getattr(config, 'ORDER_FILL_TIMEOUT_SECONDS', 90)
        t0 = time.time()
        while time.time() - t0 < timeout:
            try:
                status = self.trading_client.get_order_by_id(order_id).status
                if status == OrderStatus.FILLED:
                    return True
                if status in (OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED):
                    return False
            except Exception:
                pass
            time.sleep(2)
        return False

    def _execute_order(self, symbol: str, side: OrderSide, qty: float, order_type_label: str) -> bool:
        try:
            req = MarketOrderRequest(
                symbol=self._pos_key(symbol), qty=qty, side=side,
                time_in_force=self._order_tif(symbol),
            )
            order = self.trading_client.submit_order(order_data=req)
            self.active_orders[symbol] = order.id
            if not self._wait_fill(order.id):
                try:
                    self.trading_client.cancel_order_by_id(order.id)
                except Exception:
                    pass
                logger.error(f"{symbol} {order_type_label} NOT FILLED")
                return False
            logger.info(f"{symbol} {order_type_label} filled: {qty} {side.value}")
            self.last_order_error.pop(symbol, None)
            return True
        except Exception as e:
            self.last_order_error[symbol] = str(e)
            logger.error(f"{symbol} {order_type_label} failed: {e}")
            return False

    def close_position(self, symbol: str, qty: float, reason: str) -> bool:
        # v3.5.2: the close direction comes from the BROKER's live signed
        # qty, never from tracked state. During the PLTR pile-up the tracked
        # side was briefly wrong, which turned "closes" into orders in the
        # wrong direction - that class of bug is now impossible by
        # construction. Qty is still capped at what the broker actually
        # holds (v3.5), so a close can never overshoot into a short.
        signed = self.get_position_qty(symbol)
        live_qty = abs(signed)
        if live_qty <= 0:
            logger.info(f"{symbol} close ({reason}): broker already flat - clearing state")
            self._reset_position_state(symbol)
            return False
        side = OrderSide.SELL if signed > 0 else OrderSide.BUY
        broker_side = 'LONG' if signed > 0 else 'SHORT'
        if self.position_side.get(symbol) != broker_side:
            logger.warning(f"{symbol} close ({reason}): tracked side {self.position_side.get(symbol)} "
                           f"!= broker side {broker_side} - following the broker")
        qty = min(qty, live_qty)
        if qty <= 0:
            return False
        self.cancel_symbol_orders(symbol)
        ok = self._execute_order(symbol, side, qty, f"CLOSE({reason})")
        if not ok:
            # Fallback (v3.5.2): after 2 rejected market closes, cancel
            # resting orders again and ask Alpaca to liquidate the position
            # server-side. This endpoint is side- and qty-agnostic (it
            # closes whatever the broker holds), so it works even when the
            # order path is confused by foreign orders on the account. It
            # only ever executes a decision the exit stack already made -
            # it never decides to close on its own.
            fails = self.close_fail_count.get(symbol, 0) + 1
            self.close_fail_count[symbol] = fails
            if fails >= 2:
                try:
                    self.cancel_symbol_orders(symbol)
                    time.sleep(3)
                    self.trading_client.close_position(self._pos_key(symbol))
                    logger.warning(f"{symbol} close ({reason}): market order rejected {fails}x - "
                                   f"used broker-side liquidation instead")
                    ok = True
                except Exception as e2:
                    logger.error(f"{symbol} broker-side liquidation also failed: {e2}")
            if fails >= 3:
                send_telegram(
                    f"{symbol} close ({reason}) REJECTED {fails}x by the broker and the "
                    f"position is still OPEN. Last error: {self.last_order_error.get(symbol, '?')}. "
                    f"Check the Alpaca dashboard - and check for a SECOND trader on this account.",
                    'warning')
            if not ok:
                return False
        price = self.get_latest_price(symbol)
        if price is None:
            price = self.entry_price[symbol]
        pnl = (price - self.entry_price[symbol]) * qty \
            if broker_side == 'LONG' else \
            (self.entry_price[symbol] - price) * qty
        self.daily_realized_pnl[symbol] += pnl
        self.daily_trades[symbol].append({'time': datetime.now(timezone.utc), 'pnl': pnl, 'reason': reason})
        # v3.6: richer exit math, captured BEFORE state reset
        entry = self.entry_price[symbol]
        # ATR for the exit report: fetched fresh (there is no tracked self.atr;
        # a missing/invalid ATR must NEVER break a close - the trade is done).
        atr = 0.0
        try:
            _df = self.get_bars(symbol)
            if _df is not None and not _df.empty:
                _feats = calculate_all_indicators(
                    _df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy())
                if not _feats.empty:
                    atr = float(_feats.iloc[-1]['atr_14'])
        except Exception:
            atr = 0.0
        if atr <= 0 or np.isnan(atr):
            atr = 0.0
        peak_px = self.highest_price[symbol] if broker_side == 'LONG' else self.lowest_price[symbol]
        peak_atr = 0.0
        if atr > 0 and entry > 0 and peak_px:
            peak_atr = ((peak_px - entry) if broker_side == 'LONG' else (entry - peak_px)) / atr
        keep_pct = (pnl / (peak_atr * atr * qty) * 100.0) if (peak_atr > 0 and atr > 0) else 0.0
        r_mult = (pnl / (config.STOP_ATR_MULT * atr * qty)) if atr > 0 else 0.0
        hold_h = 0.0
        if self.entry_bar_time[symbol]:
            hold_h = (datetime.now(timezone.utc) - self.entry_bar_time[symbol]).total_seconds() / 3600.0
        # v3.6: stop-outs feed the loss-cooldown memory
        if reason == 'STOP_LOSS':
            self.last_stop_time[symbol] = time.time()
            self.stop_history.setdefault(symbol, []).append(time.time())
            cutoff = time.time() - config.REPEAT_LOSS_WINDOW_DAYS * 86400
            self.stop_history[symbol] = [t for t in self.stop_history[symbol] if t >= cutoff]
        self._reset_position_state(symbol)
        self.cooldown[symbol] = COOLDOWN_BARS
        send_telegram(
            f"{symbol} closed ({reason})\n"
            f"PnL: ${pnl:+.2f} ({r_mult:+.1f}R) | held {hold_h:.1f}h\n"
            f"peak +{peak_atr:.1f} ATR, kept {keep_pct:.0f}% of peak", 'exit')
        return True

    def _reset_position_state(self, symbol: str):
        self.in_position[symbol] = False
        self.position_side[symbol] = None
        self.entry_price[symbol] = 0.0
        self.position_qty[symbol] = 0.0
        self.highest_price[symbol] = 0.0
        self.lowest_price[symbol] = float('inf')
        self.breakeven_set[symbol] = False
        self.trailing_tp_set[symbol] = False
        self.partial_closed[symbol] = False
        self.time_partial_done[symbol] = False
        self.scale_out_1_done[symbol] = False
        self.scale_out_2_done[symbol] = False
        self.ratchet_done[symbol] = False
        self.flip_tightened[symbol] = False
        self.entry_bar_time[symbol] = None
        self.stop_loss[symbol] = None
        self.take_profit[symbol] = None
        self.close_fail_count[symbol] = 0
        self.scale_out_1_done[symbol] = False
        self.scale_out_2_done[symbol] = False
        self.ratchet_done[symbol] = False
        self.flip_tightened[symbol] = False
        self._last_pos_heartbeat[symbol] = -1
        self._last_trail_alert[symbol] = None
        self.retracement_armed[symbol] = False
        self._last_brain[symbol] = None

    # ================= Broker reconcile / adoption (v3.4) =================
    def _reconcile_positions(self):
        """Two-way sync with the broker. Runs at startup and every cycle.
        - Broker holds a position we don't track -> ADOPT it: bracket rebuilt
          from the broker's avg entry + current ATR, then managed with the
          exact same exit rules as a fresh trade. Never closed here.
        - We track a position the broker no longer holds -> clear it
          (closed while we were down, or closed manually at the broker).
        Restarts, crashes and overnight gaps can no longer orphan trades."""
        try:
            positions = self.trading_client.get_all_positions()
        except Exception as e:
            logger.error(f"Position reconcile failed (will retry next cycle): {e}")
            return

        broker = {}
        for p in positions:
            q = float(p.qty)
            broker[p.symbol] = (q if p.side.value == 'long' else -q,
                                float(p.avg_entry_price))

        # 1) adopt positions we don't track
        for pos_key, (signed_qty, avg_entry) in broker.items():
            symbol = self._match_universe_symbol(pos_key)
            if symbol is None:
                # legacy / off-universe symbol: track it for management only
                symbol = pos_key
                self._init_symbol_state(symbol)
                self.symbols.append(symbol)
                self.all_symbols.append(symbol)
                logger.info(f"Broker position in off-universe {symbol} - tracking for management only")
            if self.in_position.get(symbol):
                # Side-flip guard (v3.5.1): the broker may hold the OPPOSITE
                # side of what we track (e.g. a leftover short from the
                # two-process fight while we adopted a long). Managing it
                # with the wrong side makes every exit do the reverse of the
                # right thing, so re-adopt from the broker's truth.
                broker_side = 'LONG' if signed_qty > 0 else 'SHORT'
                if self.position_side.get(symbol) != broker_side:
                    logger.warning(f"{symbol} side mismatch: tracking {self.position_side.get(symbol)} "
                                   f"but broker holds {broker_side} - re-adopting from broker state")
                    send_telegram(
                        f"{symbol} side mismatch fixed: broker holds {broker_side} "
                        f"{abs(signed_qty)} @ ${avg_entry:.2f} - bracket rebuilt for {broker_side}.",
                        'warning')
                    self._reset_position_state(symbol)
                    self._adopt_position(symbol, signed_qty, avg_entry)
                    continue
                # Size-drift guard (v3.5.2): same side but the SIZE changed
                # and no order of mine explains it => someone/something else
                # is trading this account. The old side-only check watched
                # PLTR double 55 -> 110 -> 330 -> 660 -> 1320 and saw
                # nothing wrong. Re-sync to the broker size (never close)
                # and raise the alarm.
                tracked_qty = abs(self.position_qty.get(symbol, 0.0))
                broker_qty = abs(signed_qty)
                min_meaningful = 0.001 if self._is_crypto(symbol) else 0.5
                if tracked_qty > 0 and broker_qty > 0 \
                        and abs(broker_qty - tracked_qty) > max(min_meaningful, tracked_qty * SIZE_DRIFT_TOLERANCE):
                    logger.warning(
                        f"{symbol} size mismatch: tracking {tracked_qty} but broker holds "
                        f"{broker_qty} - no order of mine explains it. Re-syncing to broker size.")
                    now = time.time()
                    if now - self._ghost_alerted.get(symbol, 0) > RUNAWAY_ALERT_SECONDS:
                        self._ghost_alerted[symbol] = now
                        send_telegram(
                            f"⚠️ {symbol} size changed {tracked_qty} -> {broker_qty} at the broker "
                            f"and I did NOT trade it. If you didn't trade manually, ANOTHER PROCESS "
                            f"IS USING THIS ACCOUNT - find it and kill it "
                            f"(docker ps / pgrep -af live_paper_trader / other machines).",
                            'warning')
                    self.position_qty[symbol] = broker_qty
                    self.entry_price[symbol] = avg_entry
                self._check_runaway(symbol, signed_qty, avg_entry)
                continue                      # tracked and same side - leave it alone
            self._adopt_position(symbol, signed_qty, avg_entry)
            self._check_runaway(symbol, signed_qty, avg_entry)

        # 2) clear positions the broker no longer holds
        for s in list(self.all_symbols):
            if self.in_position.get(s) and self._pos_key(s) not in broker:
                logger.info(f"{s} no longer held at broker (closed while down or manually) - clearing state")
                self._reset_position_state(s)
                send_telegram(f"{s} position no longer at the broker - stopped tracking "
                              f"(closed while I was down?)", 'info')

    def _match_universe_symbol(self, pos_key: str) -> Optional[str]:
        for s in self.all_symbols:
            if s == pos_key or self._pos_key(s) == pos_key:
                return s
        return None

    def _check_runaway(self, symbol: str, signed_qty: float, avg_entry: float):
        """Runaway-size circuit breaker (v3.5.2). A position bigger than
        RUNAWAY_NOTIONAL_MULT x its sizing slice cannot have come from this
        strategy - it is either the ghost-trader pile-up or a manual trade.
        We never close it here (the exit stack owns all closes); we alert,
        once per hour per symbol."""
        if not USE_REAL_ACCOUNT_SIZING or self._last_equity <= 0 or avg_entry <= 0:
            return
        slice_cap = min(CAPITAL_PER_SYMBOL, self._last_equity * SLICE_PCT_OF_EQUITY)
        notional = abs(signed_qty) * avg_entry
        if notional <= slice_cap * RUNAWAY_NOTIONAL_MULT:
            return
        now = time.time()
        if now - self._runaway_alerted.get(symbol, 0) <= RUNAWAY_ALERT_SECONDS:
            return
        self._runaway_alerted[symbol] = now
        logger.error(f"{symbol} RUNAWAY position: ${notional:,.0f} notional is "
                     f">{RUNAWAY_NOTIONAL_MULT:.0f}x its ${slice_cap:,.0f} slice")
        send_telegram(
            f"🚨 {symbol} position is ${notional:,.0f} - {notional / slice_cap:.1f}x the max size "
            f"this strategy can open. This did NOT come from the strategy. "
            f"I will manage it with the normal exit rules (no blind close), but you should "
            f"check the Alpaca order history and look for a second trader.",
            'warning')

    def _adopt_position(self, symbol: str, signed_qty: float, avg_entry: float):
        side = 'LONG' if signed_qty > 0 else 'SHORT'
        qty = abs(signed_qty)
        if qty <= 0 or avg_entry <= 0:
            return
        self.in_position[symbol] = True
        self.position_side[symbol] = side
        self.position_qty[symbol] = qty
        self.entry_price[symbol] = avg_entry
        self.highest_price[symbol] = avg_entry
        self.lowest_price[symbol] = avg_entry
        # Time-based exits count from adoption (None would freeze them).
        self.entry_bar_time[symbol] = datetime.now(timezone.utc) if ADOPTED_TIME_LIMIT else None

        # Rebuild the bracket from the broker's avg entry + current ATR
        atr = None
        df = self.get_bars(symbol)
        if df is not None and not df.empty:
            feats = calculate_all_indicators(
                df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy())
            if not feats.empty:
                atr = float(feats.iloc[-1]['atr_14'])
        if atr is None or atr <= 0 or np.isnan(atr):
            sl_dist = avg_entry * ADOPT_FALLBACK_SL_PCT
            bracket_note = f"emergency {ADOPT_FALLBACK_SL_PCT:.0%} bracket (ATR unavailable)"
        else:
            sl_dist = STOP_ATR_MULT * atr
            bracket_note = f"ATR bracket ({STOP_ATR_MULT}x risk, {REWARD_RISK_RATIO} R:R)"
        tp_dist = sl_dist * REWARD_RISK_RATIO
        self.stop_loss[symbol] = (avg_entry - sl_dist) if side == 'LONG' else (avg_entry + sl_dist)
        self.take_profit[symbol] = (avg_entry + tp_dist) if side == 'LONG' else (avg_entry - tp_dist)

        logger.info(f"ADOPTED {symbol} {side} {qty} @ {avg_entry:.2f} - {bracket_note}: "
                    f"SL={self.stop_loss[symbol]:.2f} TP={self.take_profit[symbol]:.2f}")
        send_telegram(
            f"ADOPTED {symbol} {side} {qty} @ ${avg_entry:.2f} (existed before restart)\n"
            f"Bracket rebuilt: SL ${self.stop_loss[symbol]:.2f} | "
            f"TP ${self.take_profit[symbol]:.2f} ({bracket_note})\n"
            f"Now managed with the same rules as new trades - no blind closes.", 'entry')

    # ================= Sizing =================
    def _quality_tier(self, quality: float) -> str:
        if quality >= config.QUALITY_STRONG:
            return 'STRONG'
        if quality >= config.QUALITY_MEDIUM:
            return 'MEDIUM'
        return 'WEAK'

    def calculate_position_size(self, symbol: str, price: float, atr: float, quality: float) -> float:
        tier = self._quality_tier(quality)
        risk_pct = {'STRONG': config.RISK_PCT_STRONG,
                    'MEDIUM': config.RISK_PCT_MEDIUM,
                    'WEAK': config.RISK_PCT_WEAK}[tier]
        capital = CAPITAL_PER_SYMBOL
        if USE_REAL_ACCOUNT_SIZING:
            try:
                equity = float(self.get_account().equity)
                capital = min(CAPITAL_PER_SYMBOL, equity * SLICE_PCT_OF_EQUITY)
            except Exception as e:
                logger.warning(f"{symbol} equity fetch failed ({e}) - using default slice")
        risk_amount = capital * risk_pct
        stop_distance = STOP_ATR_MULT * atr
        if stop_distance <= 0 or price <= 0:
            return 0.0
        qty = risk_amount / stop_distance
        max_qty = (capital * config.NOTIONAL_CAP_PCT) / price
        qty = min(qty, max_qty, config.NOTIONAL_CAP_ABS / price)
        if USE_REAL_ACCOUNT_SIZING:
            # Hard cap: one entry may never use more than X% of LIVE buying
            # power. This is what stops the account from going leveraged
            # (cash negative) again. Crypto on Alpaca is NON-marginable:
            # it is limited by settled USD (non_marginable_buying_power),
            # not the margin buying_power field.
            try:
                acct = self.get_account()
                nmbp = getattr(acct, 'non_marginable_buying_power', None)
                if self._is_crypto(symbol) and nmbp is not None:
                    bp = float(nmbp)
                else:
                    bp = float(acct.buying_power)
                max_qty_bp = (bp * BUYING_POWER_USAGE_CAP) / price
                if max_qty_bp < qty:
                    logger.info(f"{symbol} size capped by live buying power ${bp:,.0f}")
                    qty = max(max_qty_bp, 0.0)
            except Exception:
                pass
        if self._is_crypto(symbol):
            qty = round(qty, 6)
        else:
            qty = int(qty)
        if qty > 0:
            logger.info(f"{symbol} size: {qty} ({tier} tier, q={quality:.2f}, slice=${capital:,.0f})")
        return qty

    def _spread_ok(self, symbol: str, price: float) -> bool:
        if self._is_crypto(symbol):
            return True
        try:
            from alpaca.data.requests import StockLatestQuoteRequest
            q = self.stock_data_client.get_stock_latest_quote(
                StockLatestQuoteRequest(symbol_or_symbols=symbol))[symbol]
            if q.ask_price and q.bid_price and q.ask_price > 0:
                return (q.ask_price - q.bid_price) / q.ask_price <= MAX_SPREAD_PCT
        except Exception:
            pass
        return True

    # ================= Entry =================
    @staticmethod
    def _parse_reason(reason: str):
        """Pull regime / sentiment / memory depth out of the brain's reason string."""
        regime, sent, n = '', 0.0, 0
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

    def _session_open_blackout(self, symbol: str) -> bool:
        """v3.6: no stock entries in the first SESSION_OPEN_NO_ENTRY_MINUTES of
        the US session - the open is where spread/volatility whipsawed entries
        all week (9 of 26 stock entries fired in the first 90 min; 4 orders
        died unfilled there)."""
        if self._is_crypto(symbol):
            return False
        mins = getattr(config, 'SESSION_OPEN_NO_ENTRY_MINUTES', 60)
        if mins <= 0:
            return False
        now = datetime.now(timezone.utc)
        if now.weekday() >= 5:
            return False
        open_min = 13 * 60 + 30                     # 9:30 ET = 13:30 UTC (EDT)
        cur_min = now.hour * 60 + now.minute
        return open_min <= cur_min < open_min + mins

    def _crypto_momentum_ok(self, symbol: str, df: pd.DataFrame, price: float, sma: float) -> bool:
        """v3.6: crypto sentiment is ALWAYS 0 (no news feed), so momentum is
        the honesty check: LONG only above the 200-bar average AND with a
        positive 24h return. Blocks knife-catching (ETH fell 2504->2411 while
        the brain kept saying regime=trend_up)."""
        if not getattr(config, 'CRYPTO_MOMENTUM_GATE', True):
            return True
        if price <= sma:
            return False
        if len(df) >= 25:
            ret_24h = price / float(df.iloc[-25]['close']) - 1.0
            return ret_24h > 0
        return False

    def _min_quality(self, symbol: str) -> float:
        """v3.6.3: crypto's quality distribution is structurally lower (no news
        feed, different memory depth), so it gets its own floor; the momentum
        gate + confirmation layer do the protective work for crypto."""
        return CRYPTO_MIN_SIGNAL_QUALITY if self._is_crypto(symbol) else config.MIN_SIGNAL_QUALITY

    def _entry_confirmation(self, symbol: str, side0: str, latest, price: float, sma: float):
        """v3.6.3: the brain votes from memory - this layer makes the CURRENT
        tape agree before money moves. Returns (ok, reason_if_blocked).
        - bar confirm:   the last closed bar must move WITH the signal
        - vwap confirm:  LONG only above today's VWAP (buyers in control)
        - no-chase:      never enter right after a spike bar (> 1.5x ATR)
        - adx:           with-trend entries need a real trend (ADX >= 20)
        - vol filter:    no entries while ATR is spiking vs its own average
        Every field is read defensively: a missing indicator never blocks."""
        ret_1 = latest.get('ret_1')
        atr_pct = latest.get('atr_pct')
        dist_vwap = latest.get('dist_vwap')
        adx = latest.get('adx_14')
        vol_ratio = latest.get('volatility_ratio')

        if VOL_FILTER_ENABLED and vol_ratio is not None and not np.isnan(vol_ratio) \
                and vol_ratio > VOL_RATIO_MAX:
            return False, f"volatility spike (ATR {vol_ratio:.1f}x its average > {VOL_RATIO_MAX:.1f})"

        if ENTRY_BAR_CONFIRM and ret_1 is not None and not np.isnan(ret_1):
            if side0 == 'LONG' and ret_1 <= 0:
                return False, f"bar confirm failed (last bar {ret_1:+.2%} - tape not with the LONG)"
            if side0 == 'SHORT' and ret_1 >= 0:
                return False, f"bar confirm failed (last bar {ret_1:+.2%} - tape not with the SHORT)"

        if ENTRY_VWAP_CONFIRM and dist_vwap is not None and not np.isnan(dist_vwap):
            if side0 == 'LONG' and dist_vwap < 0:
                return False, f"below VWAP ({dist_vwap:+.1f} ATR) - sellers in control today"
            if side0 == 'SHORT' and dist_vwap > 0:
                return False, f"above VWAP ({dist_vwap:+.1f} ATR) - buyers in control today"

        if ENTRY_NO_CHASE and ret_1 is not None and atr_pct is not None \
                and not np.isnan(ret_1) and not np.isnan(atr_pct) and atr_pct > 0:
            if abs(ret_1) > ENTRY_NO_CHASE_MAX_RANGE_ATR * atr_pct:
                return False, f"no-chase: last bar moved {abs(ret_1)/atr_pct:.1f}x ATR - entry would be chasing"

        if ENTRY_ADX_MIN > 0 and adx is not None and not np.isnan(adx):
            with_trend = ((side0 == 'LONG' and price > sma) or (side0 == 'SHORT' and price < sma))
            if with_trend and adx < ENTRY_ADX_MIN:
                return False, f"weak trend (ADX {adx:.0f} < {ENTRY_ADX_MIN:.0f})"

        return True, ''

    def check_entry(self, symbol: str) -> Tuple[Optional[str], float, str]:
        if self._session_open_blackout(symbol):
            return None, 0.0, 'session_open_blackout'
        df = self.get_bars(symbol)
        if df is None or len(df) < SMA_PERIOD + 5:
            return None, 0.0, 'no_data'
        latest = self.compute_indicators(df, symbol)
        if latest is None:
            return None, 0.0, 'stale_or_warmup'

        result = self.meta_learner.get_signal(symbol, latest, timestamp=latest['timestamp'], mode='live')
        signal, quality = result['signal'], result.get('quality', 0.0)
        reason = result.get('reason', '')
        logger.info(f"{symbol} | {signal} q={quality:.3f} | {reason}")

        price = float(latest['close'])
        sma = latest.get('sma_200')
        if sma is None or np.isnan(sma):
            return None, 0.0, 'no_sma'

        regime, sent, n_mem = self._parse_reason(reason)

        # v3.6: quality is only as good as the memory behind it. A q=0.46
        # built on 35 neighbors is NOT better than a q=0.20 built on 100 -
        # scale by depth before any gate looks at it.
        ref_n = getattr(config, 'QUALITY_MEMORY_REF_N', 80)
        eff_quality = quality * min(1.0, (n_mem / ref_n) if ref_n else 1.0) if n_mem else quality * 0.5

        # v3.6 entry analysis gates (the losing cluster was always the same:
        # BUY + trend_down + fearful sentiment + thin memory)
        if signal in ('BUY', 'SELL'):
            side0 = 'LONG' if signal == 'BUY' else 'SHORT'
            veto_long = getattr(config, 'SENTIMENT_VETO_LONG', -0.60)
            veto_short = getattr(config, 'SENTIMENT_VETO_SHORT', 0.60)
            toxic_sent = getattr(config, 'TOXIC_REGIME_SENT', -0.30)
            regime_min_q = getattr(config, 'TREND_REGIME_MIN_QUALITY', 0.45)
            if side0 == 'LONG' and sent <= veto_long:
                logger.info(f"{symbol} | {signal} q={quality:.3f} BLOCKED: extreme sentiment ({sent:+.2f} <= {veto_long:+.2f})")
                return None, eff_quality, reason
            if side0 == 'SHORT' and sent >= veto_short:
                logger.info(f"{symbol} | {signal} q={quality:.3f} BLOCKED: extreme sentiment ({sent:+.2f} >= {veto_short:+.2f})")
                return None, eff_quality, reason
            if side0 == 'LONG' and regime == 'trend_down' and sent <= toxic_sent:
                logger.info(f"{symbol} | {signal} q={quality:.3f} BLOCKED: toxic combo "
                            f"(regime=trend_down AND sent {sent:+.2f} <= {toxic_sent:+.2f})")
                return None, eff_quality, reason
            if side0 == 'SHORT' and regime == 'trend_up' and sent >= -toxic_sent:
                logger.info(f"{symbol} | {signal} q={quality:.3f} BLOCKED: toxic combo "
                            f"(regime=trend_up AND sent {sent:+.2f} >= {-toxic_sent:+.2f})")
                return None, eff_quality, reason
            if ((side0 == 'LONG' and regime == 'trend_down') or
                    (side0 == 'SHORT' and regime == 'trend_up')) and eff_quality < regime_min_q:
                logger.info(f"{symbol} | {signal} q={quality:.3f} eff={eff_quality:.3f} BLOCKED: "
                            f"counter-regime entries must be STRONG (>= {regime_min_q:.2f})")
                return None, eff_quality, reason
            if side0 == 'LONG' and self._is_crypto(symbol) and not self._crypto_momentum_ok(symbol, df, price, sma):
                logger.info(f"{symbol} | BUY q={quality:.3f} BLOCKED: crypto momentum gate "
                            f"(need price > 200-bar avg AND 24h return > 0)")
                return None, eff_quality, reason
            # v3.6.3: the tape itself must agree (bar / VWAP / no-chase / ADX / vol filter)
            ok, why = self._entry_confirmation(symbol, side0, latest, price, sma)
            if not ok:
                logger.info(f"{symbol} | {signal} q={quality:.3f} eff={eff_quality:.3f} BLOCKED: {why}")
                return None, eff_quality, reason

        # With-trend entries
        if signal == 'BUY' and price > sma:
            return 'LONG', eff_quality, reason
        if signal == 'SELL' and price < sma:
            if self._is_crypto(symbol):
                logger.info(f"{symbol} | SELL q={quality:.3f} skipped: crypto is long-only on Alpaca spot")
                return None, eff_quality, reason
            return 'SHORT', eff_quality, reason

        # Counter-trend (v3.2): allowed at reduced quality UNLESS the
        # short-term regime also disagrees (fighting both timeframes = veto).
        if signal in ('BUY', 'SELL') and eff_quality >= self._min_quality(symbol):
            opposed = ((signal == 'SELL' and regime == 'trend_up') or
                       (signal == 'BUY' and regime == 'trend_down'))
            relation = 'above' if price > sma else 'below'
            if opposed:
                logger.info(f"{symbol} | {signal} q={quality:.3f} BLOCKED: fights both timeframes "
                            f"(price {relation} 200-bar avg, regime={regime or 'unknown'})")
                return None, eff_quality, reason
            adj = eff_quality * COUNTER_TREND_PENALTY
            if adj < self._min_quality(symbol):
                logger.info(f"{symbol} | {signal} q={quality:.3f} BLOCKED: counter-trend penalty "
                            f"-> effective q={adj:.3f} below gate {self._min_quality(symbol):.2f}")
                return None, eff_quality, reason
            if signal == 'SELL' and self._is_crypto(symbol):
                logger.info(f"{symbol} | SELL q={quality:.3f} skipped: crypto is long-only on Alpaca spot")
                return None, eff_quality, reason
            side = 'LONG' if signal == 'BUY' else 'SHORT'
            logger.info(f"{symbol} | {signal} q={quality:.3f} counter-trend ALLOWED "
                        f"(penalty x{COUNTER_TREND_PENALTY} -> effective q={adj:.3f}, regime={regime or 'unknown'})")
            return side, adj, reason
        return None, eff_quality, reason

    def _loss_cooldown_active(self, symbol: str) -> Optional[str]:
        """v3.6: after a STOP_LOSS the symbol is banned for LOSS_COOLDOWN_HOURS;
        two stop-outs inside REPEAT_LOSS_WINDOW_DAYS ban it for
        REPEAT_LOSS_COOLDOWN_HOURS. (ETH was stopped 3x in 7h on a 5-min
        cooldown - never again.)"""
        now = time.time()
        hist = [t for t in self.stop_history.get(symbol, [])
                if now - t < getattr(config, 'REPEAT_LOSS_WINDOW_DAYS', 7) * 86400]
        self.stop_history[symbol] = hist
        if len(hist) >= 2:
            wait = getattr(config, 'REPEAT_LOSS_COOLDOWN_HOURS', 72) * 3600
            if now - hist[-1] < wait:
                return f"repeat-loss ban ({len(hist)} stops in {getattr(config, 'REPEAT_LOSS_WINDOW_DAYS', 7)}d)"
        last = self.last_stop_time.get(symbol, 0)
        wait = getattr(config, 'LOSS_COOLDOWN_HOURS', 24) * 3600
        if last and now - last < wait:
            return f"stop-out cooldown ({(wait - (now - last)) / 3600:.0f}h left)"
        return None

    def enter_position(self, symbol: str, side: str, quality: float, reason: str = ''):
        ban = self._loss_cooldown_active(symbol)
        if ban:
            logger.info(f"{symbol} entry skipped: {ban}")
            return
        # Full bar window: calculate_all_indicators() dropna()s the 200-bar
        # warm-up, so a short window returns an EMPTY frame (the v3.2 bug
        # that silently killed every entry).
        df = self.get_bars(symbol)
        if df is None or df.empty:
            logger.warning(f"{symbol} entry aborted: bar fetch failed")
            return
        price = self.get_latest_price(symbol)
        feats = calculate_all_indicators(
            df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy())
        if price is None:
            logger.warning(f"{symbol} entry aborted: no latest price")
            return
        if feats.empty:
            logger.warning(f"{symbol} entry aborted: indicator set empty after warm-up drop")
            return
        atr = float(feats.iloc[-1]['atr_14'])
        if atr <= 0 or np.isnan(atr):
            logger.warning(f"{symbol} entry aborted: invalid ATR ({atr})")
            return
        if not self._spread_ok(symbol, price):
            logger.info(f"{symbol} entry skipped: spread wider than {MAX_SPREAD_PCT:.1%}")
            return

        qty = self.calculate_position_size(symbol, price, atr, quality)
        if qty <= 0:
            logger.warning(f"{symbol} entry aborted: size rounds to {qty} "
                           f"(price={price:.2f} atr={atr:.2f} q={quality:.3f})")
            return

        if not self._is_crypto(symbol):
            open_positions = sum(1 for s in self.all_symbols if self.in_position[s])
            if open_positions >= MAX_POSITIONS:
                logger.info(f"{symbol} entry skipped: MAX_POSITIONS={MAX_POSITIONS} reached")
                return

        order_side = OrderSide.BUY if side == 'LONG' else OrderSide.SELL
        if not self._execute_order(symbol, order_side, qty, f"ENTRY({side})"):
            # Back off instead of re-submitting a rejected order every cycle
            # (the Aug-24 SOL "insufficient balance" storm retried for hours).
            if 'insufficient' in self.last_order_error.get(symbol, '').lower():
                self.cooldown[symbol] = ENTRY_FAIL_COOLDOWN_CYCLES
                logger.info(f"{symbol} entry rejected (insufficient funds) - backing off "
                            f"{ENTRY_FAIL_COOLDOWN_CYCLES} cycles")
            return

        time.sleep(2)
        signed = self.get_position_qty(symbol)
        actual_qty = abs(signed)
        if actual_qty <= 0:
            logger.error(f"{symbol} entry not confirmed at broker")
            return
        broker_side = 'LONG' if signed > 0 else 'SHORT'
        if broker_side != side:
            # We ordered one side but the broker shows the other: leave
            # state untouched and let the next reconcile adopt the truth.
            logger.error(f"{symbol} entry side check failed: wanted {side}, broker shows "
                         f"{broker_side} {actual_qty} - not tracking, reconcile will adopt")
            send_telegram(f"{symbol} ENTRY SIDE MISMATCH: ordered {side} but the broker shows "
                          f"{broker_side} {actual_qty} - managing from broker truth.", 'warning')
            return
        if actual_qty > qty * 1.5:
            logger.warning(f"{symbol} post-entry size {actual_qty} is bigger than my order {qty} - "
                           f"the position includes qty I did not buy (another trader?)")

        self.in_position[symbol] = True
        self.position_side[symbol] = side
        self.entry_price[symbol] = price
        self.position_qty[symbol] = actual_qty
        self.highest_price[symbol] = price
        self.lowest_price[symbol] = price
        self.entry_bar_time[symbol] = datetime.now(timezone.utc)

        self.stop_loss[symbol] = (price - STOP_ATR_MULT * atr) if side == 'LONG' else (price + STOP_ATR_MULT * atr)
        tp_distance = STOP_ATR_MULT * REWARD_RISK_RATIO * atr
        self.take_profit[symbol] = (price + tp_distance) if side == 'LONG' else (price - tp_distance)

        _reg, _sent, _n = self._parse_reason(reason)
        send_telegram(
            f"{symbol} {side} ENTRY\n"
            f"Price: ${price:.2f} | Qty: {actual_qty}\n"
            f"SL: ${self.stop_loss[symbol]:.2f} | TP: ${self.take_profit[symbol]:.2f}\n"
            f"Quality: {quality:.2f} ({self._quality_tier(quality)}) | memory n={_n}\n"
            f"Analysis: regime={_reg or '?'} | sentiment={_sent:+.2f} | "
            f"vetoes passed: sentiment, regime, momentum, session-open", 'entry')
        logger.info(f"{symbol} {side} entered @ {price:.2f} qty={actual_qty}")

    # ================= Exit management =================
    def _bars_in_position(self, symbol: str) -> float:
        if not self.entry_bar_time[symbol]:
            return 0
        return (datetime.now(timezone.utc) - self.entry_bar_time[symbol]).total_seconds() / 3600.0

    def manage_exit(self, symbol: str) -> Optional[str]:
        price = self.get_latest_price(symbol)
        if price is None:
            return None
        side = self.position_side[symbol]
        entry = self.entry_price[symbol]
        qty = abs(self.get_position_qty(symbol))
        if qty <= 0:
            self._reset_position_state(symbol)
            return None

        # 1) Hard stop / take profit — checked BEFORE anything data-dependent,
        #    so a position is never left unprotected by an indicator hiccup.
        if side == 'LONG':
            if price <= self.stop_loss[symbol]:
                return 'STOP_LOSS'
            if price >= self.take_profit[symbol]:
                return 'TAKE_PROFIT'
        else:
            if price >= self.stop_loss[symbol]:
                return 'STOP_LOSS'
            if price <= self.take_profit[symbol]:
                return 'TAKE_PROFIT'

        df = self.get_bars(symbol)   # full window: indicator dropna() needs 200+ bars
        atr = None
        if df is not None and not df.empty:
            feats = calculate_all_indicators(
                df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy())
            if not feats.empty:
                atr = float(feats.iloc[-1]['atr_14'])
        if atr is None or atr <= 0 or np.isnan(atr):
            logger.warning(f"{symbol} exit mgmt skipped: ATR unavailable (hard SL/TP still active)")
            return None

        self.highest_price[symbol] = max(self.highest_price[symbol], price)
        self.lowest_price[symbol] = min(self.lowest_price[symbol], price)

        if side == 'LONG':
            atr_profit = (price - entry) / atr
            peak_profit = (self.highest_price[symbol] - entry) / atr
        else:
            atr_profit = (entry - price) / atr
            peak_profit = (entry - self.lowest_price[symbol]) / atr

        # 2) Profit ratchet (v3.6 - replaces the breakeven lock, which fired
        #    ZERO times all week): at +PROFIT_RATCHET_ATR the stop jumps to
        #    entry + RATCHET_LOCK_ATR. Real money locked, not breakeven.
        if not self.ratchet_done[symbol] and atr_profit >= PROFIT_RATCHET_ATR:
            lock = (entry + RATCHET_LOCK_ATR * atr) if side == 'LONG' else (entry - RATCHET_LOCK_ATR * atr)
            if side == 'LONG':
                self.stop_loss[symbol] = max(self.stop_loss[symbol], lock)
            else:
                self.stop_loss[symbol] = min(self.stop_loss[symbol], lock)
            self.ratchet_done[symbol] = True
            send_telegram(f"🔒 {symbol} +{atr_profit:.1f} ATR - stop ratcheted to lock "
                          f"+{RATCHET_LOCK_ATR:.1f} ATR (${self.stop_loss[symbol]:.2f})", 'info')

        # 3) Scale-outs (v3.6 - replaces the fixed partial TP): bank a third
        #    at +SCALE_OUT_1_ATR and another at +SCALE_OUT_2_ATR, trail the rest.
        if getattr(config, 'SCALE_OUT_ENABLED', True):
            for flag, level, label in (('scale_out_1_done', config.SCALE_OUT_1_ATR, f"1/3 @ +{config.SCALE_OUT_1_ATR:.0f} ATR"),
                                       ('scale_out_2_done', config.SCALE_OUT_2_ATR, f"1/3 @ +{config.SCALE_OUT_2_ATR:.0f} ATR")):
                done = getattr(self, flag)
                if not done[symbol] and atr_profit >= level:
                    close_qty = qty * config.SCALE_OUT_PCT
                    close_qty = round(close_qty, 6) if self._is_crypto(symbol) else int(close_qty)
                    if close_qty > 0:
                        close_side = OrderSide.SELL if side == 'LONG' else OrderSide.BUY
                        if self._execute_order(symbol, close_side, close_qty, f"SCALE_OUT({label})"):
                            done[symbol] = True
                            # Re-sync tracked size from the broker so the
                            # size-drift guard doesn't mistake our own
                            # scale-out for a foreign trade.
                            try:
                                self.position_qty[symbol] = abs(self.get_position_qty(symbol))
                            except Exception:
                                self.position_qty[symbol] = max(0.0, self.position_qty[symbol] - close_qty)
                            send_telegram(f"💰 {symbol} scale-out {label}: banked {close_qty} @ ${price:.2f} - "
                                          f"trailing the rest", 'partial')
                    break   # one scale-out per cycle

        # 4) Trailing stop (hard) - v3.6: activates at +2.5 ATR, trails 2.5
        #    ATR behind price (the old 4.0/6.0 settings never fired once).
        if TRAILING_STOP_ENABLED and atr_profit >= TRAILING_STOP_ACTIVATE:
            if not self.trailing_tp_set[symbol]:
                self.trailing_tp_set[symbol] = True
                send_telegram(f"📈 {symbol} trailing stop engaged at +{atr_profit:.1f} ATR "
                              f"(trail {TRAILING_STOP_DISTANCE:.1f} ATR behind price)", 'info')
                self._last_trail_alert[symbol] = self.stop_loss[symbol]
            if side == 'LONG':
                trail = price - TRAILING_STOP_DISTANCE * atr
                self.stop_loss[symbol] = max(self.stop_loss[symbol], trail)
            else:
                trail = price + TRAILING_STOP_DISTANCE * atr
                self.stop_loss[symbol] = min(self.stop_loss[symbol], trail)
            # v3.6.2: alert each time the trail ratchets up by a meaningful
            # step, so you can watch it lock profit in near-real-time.
            last_alert = self._last_trail_alert.get(symbol)
            if last_alert is None or abs(self.stop_loss[symbol] - last_alert) >= TRAIL_ALERT_STEP_ATR * atr:
                self._last_trail_alert[symbol] = self.stop_loss[symbol]
                locked = ((self.stop_loss[symbol] - entry) / atr) if side == 'LONG' \
                    else ((entry - self.stop_loss[symbol]) / atr)
                logger.info(f"{symbol} trailing stop -> ${self.stop_loss[symbol]:.2f} (locks {locked:+.2f} ATR)")
                send_telegram(f"📈 {symbol} trailing stop moved to ${self.stop_loss[symbol]:.2f} "
                              f"- now locks {locked:+.1f} ATR", 'info')

        # 5) Retracement lock (v3.6 rebuild): arms only after a REAL peak
        #    (+2 ATR, not +0.7) and keeps 60% of it. The old version cut
        #    winners to ~+$28 crumbs after a median 1.6h hold.
        if PROFIT_PROTECTION_ENABLED and peak_profit >= RETRACEMENT_ARM_ATR:
            if not self.retracement_armed[symbol]:
                self.retracement_armed[symbol] = True
                send_telegram(
                    f"🛡 {symbol} peak +{peak_profit:.1f} ATR - retracement lock ARMED: "
                    f"if profit falls back to {RETRACEMENT_KEEP_PCT:.0%} of peak "
                    f"(+{peak_profit * RETRACEMENT_KEEP_PCT:.1f} ATR), I bank it", 'info')
            if atr_profit <= peak_profit * RETRACEMENT_KEEP_PCT:
                return 'RETRACEMENT_LOCK'

        # 6) (removed in v3.6 - the old trailing TP never fired; the v3.6
        #    trailing stop in step 4 does this job)

        # 7) Time-based partial
        if TIME_PARTIAL_ENABLED and not self.time_partial_done[symbol]:
            if self._bars_in_position(symbol) >= TIME_PARTIAL_BARS and atr_profit >= TIME_PARTIAL_PROFIT_ATR:
                close_qty = qty * 0.5
                close_qty = round(close_qty, 6) if self._is_crypto(symbol) else int(close_qty)
                if close_qty > 0:
                    close_side = OrderSide.SELL if side == 'LONG' else OrderSide.BUY
                    if self._execute_order(symbol, close_side, close_qty, "TIME_PARTIAL"):
                        self.time_partial_done[symbol] = True
                        try:
                            self.position_qty[symbol] = abs(self.get_position_qty(symbol))
                        except Exception:
                            self.position_qty[symbol] = max(0.0, self.position_qty[symbol] - close_qty)

        # 8) Signal flip + in-trade re-analysis (v3.6): the brain re-judges
        #    THIS open position every cycle, not just at entry. Flipped
        #    against while in real profit -> bank it now (no 2-flip wait).
        #    Flipped against while underwater -> tighten the stop (once).
        if SIGNAL_FLIP_EXIT_ENABLED:
            df_full = self.get_bars(symbol)
            if df_full is not None:
                latest = self.compute_indicators(df_full, symbol)
                if latest is not None:
                    result = self.meta_learner.get_signal(symbol, latest, timestamp=latest['timestamp'], mode='live')
                    current = result['signal']
                    cur_q = result.get('quality', 0.0)
                    opposite = 'SELL' if side == 'LONG' else 'BUY'
                    # v3.6.2: remember the brain's latest read for the heartbeat,
                    # and log the re-analysis verdict every cycle.
                    self._last_brain[symbol] = (current, cur_q)
                    verdict = 'FLIPPED AGAINST US' if current == opposite else 'still agrees'
                    logger.info(f"{symbol} REANALYSIS: {side} {atr_profit:+.2f} ATR "
                                f"(peak {peak_profit:.2f}) | stop=${self.stop_loss[symbol]:.2f} | "
                                f"brain={current} q={cur_q:.3f} -> {verdict}")
                    if current == opposite:
                        if cur_q >= config.MIN_SIGNAL_QUALITY and atr_profit >= config.FLIP_EXIT_PROFIT_ATR:
                            return 'SIGNAL_FLIP_PROFIT'
                        if (getattr(config, 'FLIP_TIGHTEN_UNDERWATER', True)
                                and atr_profit < 0 and not self.flip_tightened[symbol]
                                and cur_q >= config.MIN_SIGNAL_QUALITY):
                            self.flip_tightened[symbol] = True
                            tight = (price - 1.0 * atr) if side == 'LONG' else (price + 1.0 * atr)
                            if side == 'LONG':
                                self.stop_loss[symbol] = max(self.stop_loss[symbol], tight)
                            else:
                                self.stop_loss[symbol] = min(self.stop_loss[symbol], tight)
                            send_telegram(f"⚠️ {symbol}: brain flipped {current} (q={cur_q:.2f}) against your "
                                          f"{side} while underwater - stop tightened to 1 ATR "
                                          f"(${self.stop_loss[symbol]:.2f})", 'warning')
                        elif self.flip_count[symbol] == 0 and atr_profit >= 0:
                            send_telegram(f"⚠️ {symbol}: brain flipped {current} (q={cur_q:.2f}) against your "
                                          f"{side} at +{atr_profit:.1f} ATR - watching closely", 'info')
                        self.flip_count[symbol] += 1
                        if self.flip_count[symbol] >= SIGNAL_FLIP_CONFIRM:
                            self.flip_count[symbol] = 0
                            return 'SIGNAL_FLIP'
                    else:
                        self.flip_count[symbol] = 0

        # 9) SMA cross exit (disabled by config)
        if SMA_EXIT_ENABLED:
            df_full = self.get_bars(symbol)
            if df_full is not None and len(df_full) >= SMA_PERIOD + SMA_EXIT_CONFIRM_BARS + 1:
                sma = df_full['close'].rolling(SMA_PERIOD).mean()
                closes = df_full['close'].values
                sma_vals = sma.values
                if side == 'LONG' and all(closes[-i] < sma_vals[-i] for i in range(1, SMA_EXIT_CONFIRM_BARS + 1)):
                    return 'SMA_CROSS'
                if side == 'SHORT' and all(closes[-i] > sma_vals[-i] for i in range(1, SMA_EXIT_CONFIRM_BARS + 1)):
                    return 'SMA_CROSS'

        # 10) Time limit
        if TIME_LIMIT_ENABLED and self._bars_in_position(symbol) >= TIME_LIMIT_BARS:
            return 'TIME_LIMIT'

        # 11) Position heartbeat (v3.6.2): every POSITION_HEARTBEAT_BARS bars,
        #     a full status card - PnL, stop, what the brain thinks NOW, and
        #     what every exit rule is doing. This is the proof-of-life for
        #     the whole stack.
        bars_held = self._bars_in_position(symbol)
        bar_no = int(bars_held)
        if bar_no > 0 and bar_no % POSITION_HEARTBEAT_BARS == 0 \
                and self._last_pos_heartbeat.get(symbol, -1) != bar_no:
            self._last_pos_heartbeat[symbol] = bar_no
            pnl_d = atr_profit * atr * qty
            stop = self.stop_loss[symbol]
            stop_dist = (abs(price - stop) / atr) if stop else 0.0
            brain = self._last_brain.get(symbol)
            if brain:
                b_sig, b_q = brain
                b_state = 'agrees ✅' if b_sig != ('SELL' if side == 'LONG' else 'BUY') else 'FLIPPED ⚠️'
                brain_txt = f"{b_sig} q={b_q:.2f} ({b_state})"
            else:
                brain_txt = 'unavailable this cycle'
            scale_n = int(self.scale_out_1_done[symbol]) + int(self.scale_out_2_done[symbol])
            stack = (f"ratchet {'✅' if self.ratchet_done[symbol] else '⏳ +%.1f ATR' % PROFIT_RATCHET_ATR} | "
                     f"scale-outs {scale_n}/2 | "
                     f"trailing {'✅ live' if self.trailing_tp_set[symbol] else '⏳ +%.1f ATR' % TRAILING_STOP_ACTIVATE} | "
                     f"retr-lock {'🛡 armed' if self.retracement_armed[symbol] else '⏳ +%.1f ATR' % RETRACEMENT_ARM_ATR}")
            send_telegram(
                f"📊 {symbol} {side} update | bar {bar_no}/{TIME_LIMIT_BARS}\n"
                f"PnL: ${pnl_d:+.2f} ({atr_profit:+.1f} ATR) | peak +{peak_profit:.1f} ATR\n"
                f"Stop: ${stop:.2f} ({stop_dist:.1f} ATR away) | TP: ${self.take_profit[symbol]:.2f}\n"
                f"🧠 brain now: {brain_txt}\n"
                f"Stack: {stack}", 'info')
            logger.info(f"{symbol} HEARTBEAT bar {bar_no}: pnl=${pnl_d:+.2f} "
                        f"({atr_profit:+.2f} ATR) stop=${stop:.2f} brain={brain_txt}")

        return None

    # ================= Daily guards =================
    def _check_daily_reset(self, symbol: str, equity: float):
        today = datetime.now(timezone.utc).date()
        if self.daily_start_equity[symbol] is None:
            self.daily_start_equity[symbol] = equity
            self._last_reset = today
        elif getattr(self, '_last_reset', None) != today:
            self.daily_start_equity[symbol] = equity
            self.daily_realized_pnl[symbol] = 0.0
            self.daily_target_hit[symbol] = False
            self.daily_trades[symbol] = []
            self._last_reset = today

    def _daily_guard(self, symbol: str, equity: float) -> Tuple[bool, str]:
        start = self.daily_start_equity[symbol]
        if not start:
            return True, 'ok'
        change = (equity - start) / start
        if change <= -config.DAILY_LOSS_LIMIT_PCT:
            return False, f'daily_loss_limit ({change:+.1%})'
        if DAILY_PROFIT_TARGET_ENABLED and change >= DAILY_TARGET_PCT:
            if not self.daily_target_hit[symbol]:
                self.daily_target_hit[symbol] = True
                send_telegram(f"{symbol} DAILY TARGET +{change:.1%} - no new entries today", 'target')
                if LOCK_BREAKEVEN_ON_TARGET and self.in_position[symbol]:
                    side = self.position_side[symbol]
                    entry = self.entry_price[symbol]
                    if side == 'LONG':
                        self.stop_loss[symbol] = max(self.stop_loss[symbol], entry * 1.0005)
                    else:
                        self.stop_loss[symbol] = min(self.stop_loss[symbol], entry * 0.9995)
            return False, f'daily_profit_target ({change:+.1%})'
        return True, 'ok'

    # ================= Reports =================
    def _day_start_equity(self, fallback: float) -> float:
        return next((v for v in self.daily_start_equity.values() if v), fallback)

    def _send_eod(self):
        try:
            account = self.get_account()
            equity = float(account.equity)
            trades = []
            for sym, tl in self.daily_trades.items():
                for t in tl:
                    trades.append({'symbol': sym, 'pnl': t['pnl'], 'reason': t['reason']})
            open_pos = {s: self.position_side[s] for s in self.all_symbols
                        if self.in_position.get(s)}
            send_eod_report(trades, equity, self._day_start_equity(equity), open_pos)
            self.last_report_date = datetime.now(timezone.utc).date()
        except Exception as e:
            logger.error(f"EOD report failed: {e}")

    def _send_heartbeat(self):
        try:
            account = self.get_account()
            equity = float(account.equity)
            self._peak_equity = max(self._peak_equity, equity)
            open_syms = [s for s in self.all_symbols if self.in_position.get(s)]
            send_telegram(format_capital_update(
                equity, self._day_start_equity(equity), self._peak_equity,
                len(open_syms)), 'heartbeat')
        except Exception as e:
            logger.error(f"Heartbeat failed: {e}")

    # ================= Daily self-maintenance =================
    def _initial_maintenance_time(self):
        """First run: immediately if the last success is stale/missing,
        otherwise at the next daily slot (DAILY_MAINTENANCE_UTC_HOUR)."""
        if not DAILY_MAINTENANCE_ENABLED:
            return None
        now = datetime.now(timezone.utc)
        try:
            from src.maintenance import last_success
            last = last_success()
        except Exception:
            last = None
        if last is None or (now - last) > timedelta(hours=20):
            logger.info("Maintenance stale/missing (>20h) - will run in background now.")
            return now
        slot = now.replace(hour=DAILY_MAINTENANCE_UTC_HOUR, minute=0, second=0, microsecond=0)
        if slot <= now:
            slot += timedelta(days=1)
        return slot

    def _check_maintenance(self):
        """Non-blocking daily launcher. Never overlaps with itself; trading
        cycles continue while the maintenance job runs in the background."""
        if not DAILY_MAINTENANCE_ENABLED or self._next_maintenance is None:
            return

        # Reap a finished job and schedule the next daily slot
        if self._maintenance_proc is not None:
            rc = self._maintenance_proc.poll()
            if rc is None:
                return                                   # still running
            logger.info(f"Maintenance job finished (rc={rc}) - see logs/maintenance.log")
            self._maintenance_proc = None
            now = datetime.now(timezone.utc)
            slot = now.replace(hour=DAILY_MAINTENANCE_UTC_HOUR, minute=0, second=0, microsecond=0)
            self._next_maintenance = slot + timedelta(days=1) if slot <= now else slot
            return

        # Due? Launch `python -m src.maintenance` in the background
        if datetime.now(timezone.utc) >= self._next_maintenance:
            try:
                log_path = os.path.join(PROJECT_ROOT, "logs", "maintenance.log")
                fh = open(log_path, "a")
                self._maintenance_proc = subprocess.Popen(
                    [sys.executable, "-m", "src.maintenance"],
                    cwd=PROJECT_ROOT, stdout=fh, stderr=subprocess.STDOUT,
                )
                fh.close()   # child holds its own fd
                logger.info(f"Daily maintenance launched in background (pid {self._maintenance_proc.pid})")
                send_telegram("Daily maintenance started (data pump + memory outcome sync) - trading continues", 'info')
            except Exception as e:
                logger.error(f"Maintenance launch failed: {e}")
                self._maintenance_proc = None
                # retry tomorrow, don't hammer every cycle
                self._next_maintenance = datetime.now(timezone.utc) + timedelta(days=1)

    # ================= Main loop =================
    def _market_open(self) -> bool:
        try:
            clock = self.trading_client.get_clock()
            return bool(clock.is_open)
        except Exception:
            now = datetime.now(timezone.utc)
            return now.weekday() < 5 and 13 <= now.hour < 21

    def run(self):
        logger.info(f"Starting 1H multi-symbol trader {TRADER_VERSION}: stocks={self.symbols} crypto={self.crypto_symbols}")
        send_telegram(
            f"1H Trader started ({TRADER_VERSION})\nStocks: {', '.join(self.symbols) or 'none'}\n"
            f"Crypto: {', '.join(self.crypto_symbols) or 'none'}", 'info')

        while True:
            try:
                self.cycle_count += 1
                self._refresh_universe_if_needed()
                self._check_maintenance()
                if ADOPT_BROKER_POSITIONS:
                    self._reconcile_positions()   # catch mid-session changes (manual buys/sells, missed entries)
                account = self.get_account()
                equity = float(account.equity)
                self._last_equity = equity
                market_open = self._market_open()

                for symbol in self.all_symbols:
                    try:
                        if not self._is_crypto(symbol) and not market_open:
                            continue
                        self._check_daily_reset(symbol, equity)

                        if self.cooldown[symbol] > 0:
                            self.cooldown[symbol] -= 1
                            continue

                        if self.in_position[symbol]:
                            reason = self.manage_exit(symbol)
                            if reason:
                                qty = abs(self.get_position_qty(symbol))
                                if qty > 0:
                                    self.close_position(symbol, qty, reason)
                        else:
                            can_trade, why = self._daily_guard(symbol, equity)
                            if not can_trade:
                                continue
                            side, quality, _reason = self.check_entry(symbol)
                            if side and quality >= self._min_quality(symbol):
                                self.enter_position(symbol, side, quality, _reason)
                    except Exception as e:
                        logger.error(f"{symbol} cycle error: {e}")

                # Heartbeat + EOD
                if self.cycle_count % HEARTBEAT_CYCLES == 0:
                    self._send_heartbeat()
                if EOD_REPORT_ENABLED and not market_open:
                    if self.last_report_date != datetime.now(timezone.utc).date():
                        self._send_eod()

                time.sleep(CHECK_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Stopped by user")
                send_telegram("Trader stopped by user", 'warning')
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                time.sleep(60)


if __name__ == "__main__":
    MultiSymbolPaperTrader().run()
