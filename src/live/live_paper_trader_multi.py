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

TRAILING_STOP_ACTIVATE = 4.0      # ATR profit to activate hard trailing stop
TRAILING_STOP_DISTANCE = 6.0      # ATR behind price
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
RETRACEMENT_HIGH = config.RETRACEMENT_HIGH_THRESHOLD
RETRACEMENT_LOCK = config.RETRACEMENT_LOCK_THRESHOLD

DAILY_PROFIT_TARGET_ENABLED = config.DAILY_PROFIT_TARGET_PCT > 0
DAILY_TARGET_PCT = config.DAILY_PROFIT_TARGET_PCT
LOCK_BREAKEVEN_ON_TARGET = config.DAILY_TARGET_LOCK_BREAKEVEN

SIGNAL_FLIP_EXIT_ENABLED = True
SIGNAL_FLIP_CONFIRM = 2

TIME_PARTIAL_ENABLED = config.ENABLE_TIME_PARTIAL
TIME_PARTIAL_BARS = 12            # 12 hours
TIME_PARTIAL_PROFIT_ATR = config.TIME_PARTIAL_PROFIT_ATR

TIME_LIMIT_ENABLED = True
TIME_LIMIT_BARS = config.TIME_LIMIT_BARS   # 8 hours
COOLDOWN_BARS = config.COOLDOWN_BARS

MAX_DATA_AGE_SECONDS = 7200       # 1h bars: accept up to 2h old (hourly cadence)

EOD_REPORT_ENABLED = config.TELEGRAM_EOD_REPORT
HEARTBEAT_CYCLES = config.TELEGRAM_HEARTBEAT_CYCLES

# Daily self-maintenance (v3.3): the trader itself launches the data pump +
# Qdrant outcome sync once a day in the background, so the memory can never
# silently go blind again (the Aug-14 Qdrant crash wiped forward_return_4h
# from every point -> thin_memory(0) -> a blind brain standing down).
DAILY_MAINTENANCE_ENABLED = getattr(config, 'DAILY_MAINTENANCE_ENABLED', True)
DAILY_MAINTENANCE_UTC_HOUR = getattr(config, 'DAILY_MAINTENANCE_UTC_HOUR', 7)   # 08:00 WAT


class MultiSymbolPaperTrader:
    def __init__(self):
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
        self.active_orders: Dict[str, str] = {}

        self.last_report_date = None
        self.cycle_count = 0

        # Daily self-maintenance scheduler (pump + Qdrant outcome sync)
        self._maintenance_proc = None
        self._next_maintenance = self._initial_maintenance_time()
        if DAILY_MAINTENANCE_ENABLED and self._next_maintenance is not None:
            logger.info(f"Daily maintenance: next run at {self._next_maintenance.isoformat()}")

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

    def compute_indicators(self, df: pd.DataFrame) -> Optional[pd.Series]:
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
                logger.warning(f"Stale data ({bar_age/3600:.1f}h old), skipping cycle")
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

    def _wait_fill(self, order_id: str, timeout: int = 30) -> bool:
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
            return True
        except Exception as e:
            logger.error(f"{symbol} {order_type_label} failed: {e}")
            return False

    def close_position(self, symbol: str, qty: float, reason: str) -> bool:
        if qty <= 0:
            return False
        side = OrderSide.SELL if self.position_side[symbol] == 'LONG' else OrderSide.BUY
        self.cancel_symbol_orders(symbol)
        ok = self._execute_order(symbol, side, qty, f"CLOSE({reason})")
        if ok:
            price = self.get_latest_price(symbol)
            if price is None:
                price = self.entry_price[symbol]
            pnl = (price - self.entry_price[symbol]) * qty \
                if self.position_side[symbol] == 'LONG' else \
                (self.entry_price[symbol] - price) * qty
            self.daily_realized_pnl[symbol] += pnl
            self.daily_trades[symbol].append({'time': datetime.now(timezone.utc), 'pnl': pnl, 'reason': reason})
            self._reset_position_state(symbol)
            self.cooldown[symbol] = COOLDOWN_BARS
            send_telegram(f"{symbol} closed ({reason})\nPnL: ${pnl:+.2f}", 'exit')
        return ok

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
        self.entry_bar_time[symbol] = None
        self.stop_loss[symbol] = None
        self.take_profit[symbol] = None

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
        risk_amount = capital * risk_pct
        stop_distance = STOP_ATR_MULT * atr
        if stop_distance <= 0 or price <= 0:
            return 0.0
        qty = risk_amount / stop_distance
        max_qty = (capital * config.NOTIONAL_CAP_PCT) / price
        qty = min(qty, max_qty, config.NOTIONAL_CAP_ABS / price)
        if self._is_crypto(symbol):
            qty = round(qty, 6)
        else:
            qty = int(qty)
        if qty > 0:
            logger.info(f"{symbol} size: {qty} ({tier} tier, q={quality:.2f})")
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
    def check_entry(self, symbol: str) -> Tuple[Optional[str], float, str]:
        df = self.get_bars(symbol)
        if df is None or len(df) < SMA_PERIOD + 5:
            return None, 0.0, 'no_data'
        latest = self.compute_indicators(df)
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

        regime = ''
        m = re.search(r'regime=(\w+)', reason or '')
        if m:
            regime = m.group(1)

        # With-trend entries: unchanged
        if signal == 'BUY' and price > sma:
            return 'LONG', quality, reason
        if signal == 'SELL' and price < sma:
            if self._is_crypto(symbol):
                logger.info(f"{symbol} | SELL q={quality:.3f} skipped: crypto is long-only on Alpaca spot")
                return None, quality, reason
            return 'SHORT', quality, reason

        # Counter-trend (v3.2): allowed at reduced quality UNLESS the
        # short-term regime also disagrees (fighting both timeframes = veto).
        if signal in ('BUY', 'SELL') and quality >= config.MIN_SIGNAL_QUALITY:
            opposed = ((signal == 'SELL' and regime == 'trend_up') or
                       (signal == 'BUY' and regime == 'trend_down'))
            relation = 'above' if price > sma else 'below'
            if opposed:
                logger.info(f"{symbol} | {signal} q={quality:.3f} BLOCKED: fights both timeframes "
                            f"(price {relation} 200-bar avg, regime={regime or 'unknown'})")
                return None, quality, reason
            adj = quality * COUNTER_TREND_PENALTY
            if adj < config.MIN_SIGNAL_QUALITY:
                logger.info(f"{symbol} | {signal} q={quality:.3f} BLOCKED: counter-trend penalty "
                            f"-> effective q={adj:.3f} below gate {config.MIN_SIGNAL_QUALITY:.2f}")
                return None, quality, reason
            if signal == 'SELL' and self._is_crypto(symbol):
                logger.info(f"{symbol} | SELL q={quality:.3f} skipped: crypto is long-only on Alpaca spot")
                return None, quality, reason
            side = 'LONG' if signal == 'BUY' else 'SHORT'
            logger.info(f"{symbol} | {signal} q={quality:.3f} counter-trend ALLOWED "
                        f"(penalty x{COUNTER_TREND_PENALTY} -> effective q={adj:.3f}, regime={regime or 'unknown'})")
            return side, adj, reason
        return None, quality, reason

    def enter_position(self, symbol: str, side: str, quality: float):
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
            return

        time.sleep(2)
        actual_qty = abs(self.get_position_qty(symbol))
        if actual_qty <= 0:
            logger.error(f"{symbol} entry not confirmed at broker")
            return

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

        send_telegram(
            f"{symbol} {side} ENTRY\n"
            f"Price: ${price:.2f} | Qty: {actual_qty}\n"
            f"SL: ${self.stop_loss[symbol]:.2f} | TP: ${self.take_profit[symbol]:.2f}\n"
            f"Quality: {quality:.2f} ({self._quality_tier(quality)})", 'entry')
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

        # 2) Breakeven lock
        if not self.breakeven_set[symbol] and atr_profit >= BREAKEVEN_ATR:
            if side == 'LONG':
                self.stop_loss[symbol] = max(self.stop_loss[symbol], entry * 1.0005)
            else:
                self.stop_loss[symbol] = min(self.stop_loss[symbol], entry * 0.9995)
            self.breakeven_set[symbol] = True
            send_telegram(f"{symbol} stop -> breakeven (+{atr_profit:.1f} ATR)", 'info')

        # 3) Partial take profit
        if config.ENABLE_PARTIAL_TAKE_PROFIT and not self.partial_closed[symbol]:
            if side == 'LONG':
                tp_prog = (price - entry) / max(self.take_profit[symbol] - entry, 1e-9)
            else:
                tp_prog = (entry - price) / max(entry - self.take_profit[symbol], 1e-9)
            if tp_prog >= PARTIAL_TP_THRESHOLD:
                close_qty = qty * PARTIAL_CLOSE_PCT
                close_qty = round(close_qty, 6) if self._is_crypto(symbol) else int(close_qty)
                if close_qty > 0:
                    close_side = OrderSide.SELL if side == 'LONG' else OrderSide.BUY
                    if self._execute_order(symbol, close_side, close_qty, "PARTIAL_TP"):
                        self.partial_closed[symbol] = True
                        send_telegram(f"{symbol} partial TP: closed {close_qty}", 'partial')

        # 4) Trailing stop (hard)
        if TRAILING_STOP_ENABLED and atr_profit >= TRAILING_STOP_ACTIVATE:
            if side == 'LONG':
                trail = price - TRAILING_STOP_DISTANCE * atr
                self.stop_loss[symbol] = max(self.stop_loss[symbol], trail)
            else:
                trail = price + TRAILING_STOP_DISTANCE * atr
                self.stop_loss[symbol] = min(self.stop_loss[symbol], trail)

        # 5) Profit retracement lock
        if PROFIT_PROTECTION_ENABLED and peak_profit >= RETRACEMENT_HIGH:
            if atr_profit <= RETRACEMENT_LOCK:
                return 'RETRACEMENT_LOCK'

        # 6) Trailing take-profit
        if config.ENABLE_TRAILING_TP and not self.trailing_tp_set[symbol] and atr_profit >= TRAILING_TP_ACTIVATE:
            self.trailing_tp_set[symbol] = True
            send_telegram(f"{symbol} trailing TP armed at +{atr_profit:.1f} ATR", 'info')
        if self.trailing_tp_set[symbol]:
            if side == 'LONG':
                trail_tp = self.highest_price[symbol] - TRAILING_TP_DISTANCE * atr
                if price <= trail_tp:
                    return 'TRAILING_TP'
            else:
                trail_tp = self.lowest_price[symbol] + TRAILING_TP_DISTANCE * atr
                if price >= trail_tp:
                    return 'TRAILING_TP'

        # 7) Time-based partial
        if TIME_PARTIAL_ENABLED and not self.time_partial_done[symbol]:
            if self._bars_in_position(symbol) >= TIME_PARTIAL_BARS and atr_profit >= TIME_PARTIAL_PROFIT_ATR:
                close_qty = qty * 0.5
                close_qty = round(close_qty, 6) if self._is_crypto(symbol) else int(close_qty)
                if close_qty > 0:
                    close_side = OrderSide.SELL if side == 'LONG' else OrderSide.BUY
                    if self._execute_order(symbol, close_side, close_qty, "TIME_PARTIAL"):
                        self.time_partial_done[symbol] = True

        # 8) Signal flip (2 consecutive opposite signals)
        if SIGNAL_FLIP_EXIT_ENABLED:
            df_full = self.get_bars(symbol)
            if df_full is not None:
                latest = self.compute_indicators(df_full)
                if latest is not None:
                    result = self.meta_learner.get_signal(symbol, latest, timestamp=latest['timestamp'], mode='live')
                    current = result['signal']
                    opposite = 'SELL' if side == 'LONG' else 'BUY'
                    if current == opposite:
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
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(
                    os.path.abspath(__file__))))
                log_path = os.path.join(project_root, "logs", "maintenance.log")
                fh = open(log_path, "a")
                self._maintenance_proc = subprocess.Popen(
                    [sys.executable, "-m", "src.maintenance"],
                    cwd=project_root, stdout=fh, stderr=subprocess.STDOUT,
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
        logger.info(f"Starting 1H multi-symbol trader: stocks={self.symbols} crypto={self.crypto_symbols}")
        send_telegram(
            f"1H Trader started\nStocks: {', '.join(self.symbols) or 'none'}\n"
            f"Crypto: {', '.join(self.crypto_symbols) or 'none'}", 'info')

        while True:
            try:
                self.cycle_count += 1
                self._refresh_universe_if_needed()
                self._check_maintenance()
                account = self.get_account()
                equity = float(account.equity)
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
                            if side and quality >= config.MIN_SIGNAL_QUALITY:
                                self.enter_position(symbol, side, quality)
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
