#!/usr/bin/env python3
"""
src/live/live_paper_trader_multi.py  (v3)
Multi-symbol Alpaca paper trader - universe-driven + crypto.

v3 upgrades (on top of v2)
--------------------------
- UNIVERSE: symbols come from the DB-backed SymbolManager (not hardcoded).
  mode 'manual' = all active symbols; mode 'auto' = the DailySelector picks
  the top-N most favourable symbols each morning (score persisted with
  reasons). Symbols with open positions stay managed until flat.
- CRYPTO: BTC/USD & co. trade 24/7 with fractional quantities and GTC
  orders; data via Alpaca crypto feed; stocks keep market-hours gating.
- Everything from v2 kept: quality gate, tier sizing, daily profit target,
  buffered sma_cross, freshness guard, Telegram EOD report.
"""
import os
import time
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
from requests.exceptions import ConnectionError, Timeout

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.common.exceptions import APIError

from config.settings import config
from src.models.meta_learner import MetaLearner
from src.backtester.risk_gate import RiskGate
from src.ingestion.indicator_calculator import calculate_all_indicators
from src.universe.symbol_manager import SymbolManager
from src.utils.logger import setup_logger
from src.utils.telegram import send_telegram
from src.utils import reporter

logger = setup_logger("LiveTraderMulti", "logs/live_trader_multi.log")

MAX_RETRIES = 3
RETRY_DELAY = 5
ORDER_POLL_TIMEOUT = 30

USE_YAHOO_FALLBACK = True
try:
    import yfinance as yf
except ImportError:
    yf = None
    USE_YAHOO_FALLBACK = False


def _new_position(pos_type, entry_price, size, sl, tp, entry_time, atr):
    return {
        'type': pos_type, 'entry_price': entry_price, 'size': size,
        'stop_loss': sl, 'take_profit': tp, 'entry_time': entry_time,
        'highest_price': entry_price, 'lowest_price': entry_price, 'atr': atr,
        'partial_closed': False, 'trailing_tp_activated': False,
        'trailing_tp_distance': None, 'time_partial': False,
        'retracement_activated': False, 'retracement_stop': None,
    }


class LiveTraderMulti:
    def __init__(self, symbols=None, initial_capital=100000):
        self.mgr = SymbolManager()
        self.meta_learner = MetaLearner()
        self.trading_client = TradingClient(
            api_key=os.getenv("ALPACA_API_KEY"),
            secret_key=os.getenv("ALPACA_SECRET_KEY"), paper=True)
        self.data_client = StockHistoricalDataClient(
            api_key=os.getenv("ALPACA_API_KEY"), secret_key=os.getenv("ALPACA_SECRET_KEY"))
        self.crypto_client = CryptoHistoricalDataClient(
            api_key=os.getenv("ALPACA_API_KEY"), secret_key=os.getenv("ALPACA_SECRET_KEY"))

        # ---- Universe ----
        self.selector = None
        if config.UNIVERSE_MODE == 'auto':
            try:
                from src.universe.selector import DailySelector
                self.selector = DailySelector()
            except Exception as e:
                logger.error(f"Selector unavailable ({e}) - falling back to manual universe")

        self.initial_capital = initial_capital
        self.symbols = symbols or self._resolve_universe()
        if not self.symbols:
            self.symbols = list(config.symbols)
        logger.info(f"Trading universe ({config.UNIVERSE_MODE}): {self.symbols}")

        self.capital_per_symbol = initial_capital / max(1, len(self.symbols))
        self.risk_gates = {s: RiskGate(self.capital_per_symbol) for s in self.symbols}
        self.positions = {s: None for s in self.symbols}
        self.cooldown = {s: 0 for s in self.symbols}
        self.pending_exit = {s: False for s in self.symbols}
        self.stale_alerted = {s: False for s in self.symbols}
        self.running = True
        self.cycle_count = 0
        self.last_date = None
        self.market_was_open = False
        self.daily_target_announced = False

        self.day_start_equity = self.get_current_equity()
        self.peak_equity = self.day_start_equity
        self.journal = []
        self.day_trades = []

        for sym in self.symbols:
            self.cancel_open_orders(sym)
        self.reconcile_all_positions()

    # ------------------------------------------------------------------
    # Universe
    # ------------------------------------------------------------------
    def _resolve_universe(self):
        if self.selector is not None:
            try:
                selected = self.selector.select(top_n=config.TOP_N_SYMBOLS)
                return [r['symbol'] for r in selected]
            except Exception as e:
                logger.error(f"Auto selection failed: {e} - using active symbols")
        return self.mgr.get_active()

    @staticmethod
    def _is_crypto(symbol: str) -> bool:
        return '/' in symbol

    @staticmethod
    def _pos_key(symbol: str) -> str:
        """Alpaca position endpoint wants BTCUSD, orders want BTC/USD."""
        return symbol.replace('/', '')

    def _refresh_universe_if_needed(self):
        """Auto mode: re-select each new day; keep managing open positions."""
        if self.selector is None:
            return
        today = datetime.now().date()
        if self.last_date is not None and today == self.last_date:
            return
        try:
            selected = self.selector.select(top_n=config.TOP_N_SYMBOLS)
            new_syms = [r['symbol'] for r in selected]
            open_syms = [s for s, p in self.positions.items() if p is not None]
            merged = list(dict.fromkeys(new_syms + open_syms))
            for s in merged:
                if s not in self.positions:
                    self.positions[s] = None
                    self.cooldown[s] = 0
                    self.pending_exit[s] = False
                    self.stale_alerted[s] = False
                    self.risk_gates[s] = RiskGate(self.initial_capital / max(1, len(merged)))
            self.symbols = merged
            send_telegram("📋 Today's selection: " + ", ".join(
                f"{r['symbol']}({r['score']:.2f})" for r in selected), kind='brain')
        except Exception as e:
            logger.error(f"Universe refresh failed: {e}")

    # ------------------------------------------------------------------
    # API helpers
    # ------------------------------------------------------------------
    def _retry_call(self, func, *args, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except (ConnectionError, Timeout, APIError) as e:
                logger.warning(f"API call failed ({attempt+1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    raise
        return None

    def get_current_equity(self):
        try:
            return float(self._retry_call(self.trading_client.get_account).equity)
        except Exception as e:
            logger.error(f"get_account failed: {e}")
            return sum(self.risk_gates[s].capital for s in self.risk_gates)

    def get_account_buying_power(self):
        try:
            return float(self._retry_call(self.trading_client.get_account).buying_power)
        except Exception:
            return 0

    def get_position(self, symbol):
        try:
            return self._retry_call(self.trading_client.get_position, self._pos_key(symbol))
        except APIError as e:
            if "position does not exist" in str(e):
                return None
            logger.error(f"get_position {symbol}: {e}")
            return None
        except Exception:
            return None

    @staticmethod
    def side_str(pos):
        if pos is None:
            return None
        side = pos.side
        return side.value if hasattr(side, 'value') else str(side).lower()

    def cancel_open_orders(self, symbol):
        try:
            orders = self._retry_call(self.trading_client.get_orders)
            if not orders:
                return
            for order in orders:
                if order.symbol not in (symbol, self._pos_key(symbol)):
                    continue
                try:
                    self._retry_call(self.trading_client.cancel_order_by_id, order.id)
                    logger.info(f"Cancelled order {order.id} for {symbol}")
                except Exception as e:
                    logger.warning(f"Cancel {order.id} failed: {e}")
        except Exception as e:
            logger.error(f"cancel_open_orders {symbol}: {e}")

    def submit_order_with_retry(self, order):
        return self._retry_call(self.trading_client.submit_order, order)

    # ------------------------------------------------------------------
    # Data
    # ------------------------------------------------------------------
    @staticmethod
    def _resample_5min(df):
        df = df.set_index('timestamp').resample('5min').agg({
            'open': 'first', 'high': 'max', 'low': 'min',
            'close': 'last', 'volume': 'sum'}).dropna().reset_index()
        if not df.empty:
            last_time = df.iloc[-1]['timestamp']
            if pd.Timestamp.now(tz='UTC') - last_time < pd.Timedelta(minutes=5):
                df = df.iloc[:-1]
        return df

    def get_bars_for_symbol(self, symbol):
        end = datetime.now()
        start = end - timedelta(days=7)
        source = 'alpaca'

        if self._is_crypto(symbol):
            df = pd.DataFrame()
            try:
                req = CryptoBarsRequest(symbol_or_symbols=[symbol],
                                        timeframe=TimeFrame.Minute,
                                        start=start, end=end, limit=10000)
                bars = self._retry_call(self.crypto_client.get_crypto_bars, req).data
                if symbol in bars and len(bars[symbol]) > 0:
                    df = pd.DataFrame([{
                        'timestamp': b.timestamp, 'open': b.open, 'high': b.high,
                        'low': b.low, 'close': b.close, 'volume': b.volume,
                    } for b in bars[symbol]]).sort_values('timestamp').reset_index(drop=True)
                    df = self._resample_5min(df)
                    source = 'alpaca-crypto'
            except Exception as e:
                logger.error(f"{symbol}: crypto fetch failed: {e}")
        else:
            def _fetch_alpaca():
                for feed in ('sip', 'iex'):
                    try:
                        req = StockBarsRequest(symbol_or_symbols=[symbol],
                                               timeframe=TimeFrame.Minute,
                                               start=start, end=end, limit=10000, feed=feed)
                        bars = self._retry_call(self.data_client.get_stock_bars, req).data
                        if symbol in bars and len(bars[symbol]) > 0:
                            df = pd.DataFrame([{
                                'timestamp': b.timestamp, 'open': b.open, 'high': b.high,
                                'low': b.low, 'close': b.close, 'volume': b.volume,
                            } for b in bars[symbol]]).sort_values('timestamp').reset_index(drop=True)
                            return self._resample_5min(df)
                    except Exception as e:
                        logger.warning(f"{symbol}: feed {feed} failed: {e}")
                return pd.DataFrame()

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                try:
                    df = ex.submit(_fetch_alpaca).result(timeout=config.DATA_FETCH_TIMEOUT)
                except Exception as e:
                    logger.error(f"{symbol}: Alpaca fetch error: {e}")
                    df = pd.DataFrame()

            if df.empty and USE_YAHOO_FALLBACK and yf is not None:
                try:
                    source = 'yahoo'
                    ydf = yf.Ticker(symbol).history(period="5d", interval="5m")
                    if not ydf.empty:
                        ydf = ydf.reset_index()
                        ydf['timestamp'] = ydf['Datetime'].dt.tz_convert('UTC')
                        df = ydf.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low',
                                                 'Close': 'close', 'Volume': 'volume'})[
                            ['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                except Exception as e:
                    logger.error(f"{symbol}: Yahoo fallback failed: {e}")

        if df.empty:
            return df, source, None
        lag = (pd.Timestamp.now(tz='UTC') - df.iloc[-1]['timestamp']).total_seconds()
        logger.info(f"{symbol}: {len(df)} bars ({source}), lag={lag/60:.1f} min")
        return df, source, lag

    def is_market_open(self):
        try:
            return bool(self._retry_call(self.trading_client.get_clock).is_open)
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------
    def reconcile_all_positions(self):
        for sym in list(self.symbols):
            broker_pos = self.get_position(sym)
            if broker_pos is None:
                if self.positions.get(sym) is not None:
                    self.positions[sym] = None
                    self.pending_exit[sym] = False
                continue
            side = self.side_str(broker_pos)
            qty = abs(float(broker_pos.qty))
            entry = float(broker_pos.avg_entry_price)
            if self.positions.get(sym) is None:
                self.positions[sym] = _new_position(
                    'LONG' if side == 'long' else 'SHORT',
                    entry, qty, 0.0, 0.0, pd.Timestamp.now(tz='UTC'), None)
            else:
                self.positions[sym]['size'] = qty

    def daily_resync_if_needed(self):
        today = datetime.now().date()
        if self.last_date is None:
            self.last_date = today
            return
        if today != self.last_date:
            logger.info(f"New day ({today}) - full resync.")
            self.last_date = today
            self.day_trades = []
            self.day_start_equity = self.get_current_equity()
            self.daily_target_announced = False
            for g in self.risk_gates.values():
                g.profit_target_hit = False
            self._refresh_universe_if_needed()
            self.reconcile_all_positions()

    def log_trade(self, trade_data):
        self.journal.append(trade_data)
        self.day_trades.append(trade_data)
        try:
            pd.DataFrame(self.journal).to_csv("logs/trade_journal.csv", index=False)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Exits
    # ------------------------------------------------------------------
    def _poll_position_closed(self, symbol, timeout=ORDER_POLL_TIMEOUT):
        start = time.time()
        while time.time() - start < timeout:
            if self.get_position(symbol) is None:
                return True
            time.sleep(1)
        return False

    def flatten_symbol(self, symbol, current_price, current_time, reason="manual"):
        self.cancel_open_orders(symbol)
        broker_pos = self.get_position(symbol)
        if broker_pos is None:
            self.positions[symbol] = None
            self.pending_exit[symbol] = False
            return

        live_qty = abs(float(broker_pos.qty))
        side = self.side_str(broker_pos)
        pos = self.positions.get(symbol)
        logger.info(f"{symbol}: flatten {side} x{live_qty} ({reason})")

        try:
            self._retry_call(self.trading_client.close_position, self._pos_key(symbol))
            self.pending_exit[symbol] = True
        except Exception as e:
            logger.error(f"{symbol}: close_position failed: {e} - market order fallback")
            self._close_with_market_order(symbol, live_qty, side)
            return

        if self._poll_position_closed(symbol):
            entry = pos['entry_price'] if pos else current_price
            pnl = ((current_price - entry) if side == 'long' else (entry - current_price)) * live_qty
            pnl_pct = pnl / (entry * live_qty) * 100 if entry * live_qty else 0
            self.log_trade({'symbol': symbol, 'side': side, 'qty': live_qty,
                            'entry': entry, 'exit': current_price, 'pnl': pnl,
                            'reason': reason, 'exit_time': str(current_time)})
            if symbol in self.risk_gates:
                self.risk_gates[symbol].record_realized_pnl(pnl)
            send_telegram(
                f"EXIT {symbol} ({reason})\nSide: {side.upper()} x{live_qty}\n"
                f"Entry ${entry:.2f} -> Exit ${current_price:.2f}\nPnL: ${pnl:+,.2f} ({pnl_pct:+.2f}%)",
                kind='exit')
            self.positions[symbol] = None
            self.pending_exit[symbol] = False
            self.cooldown[symbol] = config.COOLDOWN_BARS
        else:
            send_telegram(f"{symbol}: exit pending, still open after {ORDER_POLL_TIMEOUT}s", kind='warning')

    def _order_tif(self, symbol):
        return TimeInForce.GTC if self._is_crypto(symbol) else TimeInForce.DAY

    def _close_with_market_order(self, symbol, qty, side):
        order_side = OrderSide.SELL if side == 'long' else OrderSide.BUY
        try:
            order = MarketOrderRequest(symbol=symbol, qty=qty, side=order_side,
                                       time_in_force=self._order_tif(symbol))
            submitted = self.submit_order_with_retry(order)
            if submitted and hasattr(submitted, 'id'):
                if self._poll_position_closed(symbol):
                    send_telegram(f"EXIT {symbol} (fallback) - closed.", kind='exit')
                    self.positions[symbol] = None
                    self.cooldown[symbol] = config.COOLDOWN_BARS
                    return
            send_telegram(f"CRITICAL: failed to close {symbol}!", kind='critical')
        except Exception as e:
            send_telegram(f"CRITICAL closing {symbol}: {e}", kind='critical')
        self.pending_exit[symbol] = False

    def _close_partial(self, symbol, qty, price, reason):
        if self._is_crypto(symbol):
            qty = round(qty, 6)
        else:
            qty = int(round(qty))
        if qty <= 0:
            return
        broker_pos = self.get_position(symbol)
        if broker_pos is None:
            return
        side = self.side_str(broker_pos)
        live_qty = abs(float(broker_pos.qty))
        if qty >= live_qty:
            self.flatten_symbol(symbol, price, price, reason=reason)
            return
        order_side = OrderSide.SELL if side == 'long' else OrderSide.BUY
        try:
            order = MarketOrderRequest(symbol=symbol, qty=qty, side=order_side,
                                       time_in_force=self._order_tif(symbol))
            submitted = self.submit_order_with_retry(order)
            if submitted and hasattr(submitted, 'id'):
                send_telegram(f"PARTIAL EXIT {symbol} ({reason}): {qty} @ ${price:.2f}", kind='partial')
                time.sleep(2)
                new_pos = self.get_position(symbol)
                if new_pos is None:
                    self.positions[symbol] = None
                    self.cooldown[symbol] = config.COOLDOWN_BARS
                elif self.positions[symbol] is not None:
                    self.positions[symbol]['size'] = abs(float(new_pos.qty))
        except Exception as e:
            logger.error(f"{symbol}: partial close failed: {e}")

    # ------------------------------------------------------------------
    # Daily profit target
    # ------------------------------------------------------------------
    def _check_daily_profit_target(self, equity):
        if config.DAILY_PROFIT_TARGET_PCT <= 0:
            return False
        day_pct = (equity - self.day_start_equity) / self.day_start_equity
        if day_pct >= config.DAILY_PROFIT_TARGET_PCT:
            if not self.daily_target_announced:
                self.daily_target_announced = True
                send_telegram(
                    f"🎯 DAILY PROFIT TARGET HIT: {day_pct:+.2%} "
                    f"(target {config.DAILY_PROFIT_TARGET_PCT:.2%}).\n"
                    f"No new trades today - protecting the win.", kind='target')
                if config.DAILY_TARGET_LOCK_BREAKEVEN:
                    for sym, pos in self.positions.items():
                        if pos is None:
                            continue
                        if pos['type'] == 'LONG' and pos['stop_loss'] < pos['entry_price']:
                            pos['stop_loss'] = pos['entry_price']
                        elif pos['type'] == 'SHORT' and pos['stop_loss'] > pos['entry_price']:
                            pos['stop_loss'] = pos['entry_price']
                    send_telegram("Open positions locked to breakeven.", kind='target')
            return True
        return False

    # ------------------------------------------------------------------
    # Core per-symbol logic
    # ------------------------------------------------------------------
    def process_symbol(self, symbol):
        self.daily_resync_if_needed()

        if self.pending_exit.get(symbol):
            if self.get_position(symbol) is None:
                self.pending_exit[symbol] = False
                self.positions[symbol] = None
                self.cooldown[symbol] = config.COOLDOWN_BARS
            return

        if self.cooldown.get(symbol, 0) > 0:
            self.cooldown[symbol] -= 1
            return

        self.reconcile_all_positions()

        df, source, lag = self.get_bars_for_symbol(symbol)
        if df.empty:
            logger.warning(f"{symbol}: no data")
            return

        if lag is not None and lag > config.MAX_DATA_AGE_SECONDS:
            logger.critical(f"{symbol}: STALE data ({lag/60:.1f} min old) - not trading")
            if not self.stale_alerted.get(symbol):
                self.stale_alerted[symbol] = True
                send_telegram(f"{symbol}: market data stale ({lag/60:.0f} min, source={source}). "
                              f"Trading paused until fresh.", kind='warning')
            return
        self.stale_alerted[symbol] = False

        df = calculate_all_indicators(df)
        df['sma_200'] = df['close'].rolling(200).mean()
        df = df.dropna(subset=['sma_200'])
        if df.empty:
            return

        row = df.iloc[-1]
        price, t, atr, sma200 = row['close'], row['timestamp'], row['atr_14'], row['sma_200']

        equity = self.get_current_equity()
        self.peak_equity = max(self.peak_equity, equity)
        drawdown = (self.peak_equity - equity) / self.peak_equity if self.peak_equity else 0
        if drawdown >= config.MAX_DRAWDOWN_PCT:
            send_telegram(f"MAX DRAWDOWN {drawdown:.2%} - flattening everything.", kind='critical')
            for sym in list(self.symbols):
                self.flatten_symbol(sym, price, t, reason="max_drawdown")
            return

        gate = self.risk_gates.setdefault(symbol, RiskGate(equity / max(1, len(self.symbols))))
        gate.capital = equity / max(1, len(self.symbols))
        gate.reset_daily_if_new_day(t)
        if not gate.check_daily_loss_limit(equity):
            logger.warning(f"{symbol}: daily loss limit - flattening.")
            self.flatten_symbol(symbol, price, t, reason="daily_loss_limit")
            return

        daily_target_hit = self._check_daily_profit_target(equity)

        result = self.meta_learner.get_signal(symbol, row, timestamp=t, mode='live')
        signal, prob = result['signal'], result['confidence']
        quality = result.get('quality', 0.0)
        conviction = abs(prob - 0.5)
        logger.info(f"{symbol} | {signal} prob={prob:.4f} q={quality:.3f} | {result.get('reason','')}")

        vol_ok = (atr >= config.MIN_ATR_THRESHOLD) if config.VOLATILITY_FILTER_ENABLED else True
        conviction_ok = conviction >= config.ENTRY_CONVICTION_MARGIN
        quality_ok = quality >= config.MIN_SIGNAL_QUALITY

        broker_pos = self.get_position(symbol)

        # ===================== ENTRY =====================
        if broker_pos is None:
            if daily_target_hit:
                return
            if not (vol_ok and conviction_ok and quality_ok):
                return
            direction = None
            if signal == 'BUY' and price > sma200:
                direction = 'LONG'
            elif signal == 'SELL' and price < sma200:
                direction = 'SHORT'
            if direction is None:
                return

            size, sl, tp, tier = gate.size_with_tier(price, atr, quality, direction)
            qty = round(size, 6) if self._is_crypto(symbol) else int(round(size))
            if qty <= 0:
                return
            if qty * price > self.get_account_buying_power():
                logger.warning(f"{symbol}: insufficient buying power")
                return

            side = OrderSide.BUY if direction == 'LONG' else OrderSide.SELL
            try:
                order = MarketOrderRequest(symbol=symbol, qty=qty, side=side,
                                           time_in_force=self._order_tif(symbol))
                submitted = self.submit_order_with_retry(order)
                if submitted and hasattr(submitted, 'id'):
                    self.positions[symbol] = _new_position(direction, price, qty, sl, tp, t, atr)
                    send_telegram(
                        f"{direction} ENTRY {symbol} [{tier} signal q={quality:.2f}]\n"
                        f"{qty} @ ${price:.2f}\nSL ${sl:.2f} | TP ${tp:.2f}\n"
                        f"{result.get('reason','')}", kind='entry')
            except Exception as e:
                logger.error(f"{symbol}: entry order failed: {e}")

        # ===================== MANAGE POSITION =====================
        else:
            pos = self.positions[symbol]
            if pos is None:
                pos = _new_position('LONG' if self.side_str(broker_pos) == 'long' else 'SHORT',
                                    float(broker_pos.avg_entry_price), abs(float(broker_pos.qty)),
                                    0.0, 0.0, t, atr or 0.0)
                self.positions[symbol] = pos

            pos_atr = pos.get('atr') or atr or 0.0
            is_long = pos['type'] == 'LONG'
            best = max(pos['highest_price'], price) if is_long else min(pos['lowest_price'], price)
            if is_long:
                pos['highest_price'] = best
            else:
                pos['lowest_price'] = best
            profit_atr = ((price - pos['entry_price']) if is_long else (pos['entry_price'] - price)) / pos_atr if pos_atr > 0 else 0
            tp_distance = abs(pos['take_profit'] - pos['entry_price'])
            profit_to_tp = (abs(price - pos['entry_price']) / tp_distance) if tp_distance > 0 and (
                (is_long and price > pos['entry_price']) or (not is_long and price < pos['entry_price'])) else 0

            if profit_atr >= config.BREAKEVEN_ATR_MULTIPLE:
                if is_long and pos['stop_loss'] < pos['entry_price']:
                    pos['stop_loss'] = pos['entry_price']
                elif not is_long and pos['stop_loss'] > pos['entry_price']:
                    pos['stop_loss'] = pos['entry_price']

            if profit_atr >= 4.0:
                trail = 2.5 * pos_atr if profit_atr >= 6.0 else 4.0 * pos_atr
                new_sl = (pos['highest_price'] - trail) if is_long else (pos['lowest_price'] + trail)
                if (is_long and new_sl > pos['stop_loss']) or (not is_long and new_sl < pos['stop_loss']):
                    pos['stop_loss'] = new_sl

            if config.ENABLE_PARTIAL_TAKE_PROFIT and not pos['partial_closed'] and profit_to_tp >= config.PARTIAL_TP_THRESHOLD:
                self._close_partial(symbol, pos['size'] * config.PARTIAL_CLOSE_PCT, price, "partial_tp")
                pos['stop_loss'] = pos['entry_price']
                pos['partial_closed'] = True

            if config.ENABLE_PROFIT_DRAWDOWN_PROTECTION and not pos['retracement_activated'] and profit_to_tp >= config.RETRACEMENT_HIGH_THRESHOLD:
                lock = tp_distance * config.RETRACEMENT_LOCK_THRESHOLD
                pos['retracement_stop'] = pos['entry_price'] + lock if is_long else pos['entry_price'] - lock
                pos['retracement_activated'] = True

            if config.ENABLE_TRAILING_TP and profit_atr >= config.TRAILING_TP_ATR_TRIGGER:
                if not pos['trailing_tp_activated']:
                    pos['trailing_tp_activated'] = True
                    pos['trailing_tp_distance'] = config.TRAILING_TP_DISTANCE_ATR * pos_atr
                new_tp = (pos['highest_price'] - pos['trailing_tp_distance']) if is_long else (pos['lowest_price'] + pos['trailing_tp_distance'])
                if (is_long and new_tp > pos['take_profit']) or (not is_long and new_tp < pos['take_profit']):
                    pos['take_profit'] = new_tp

            bar_age = (t - pos['entry_time']).total_seconds() / (config.BAR_MINUTES * 60)
            if config.ENABLE_TIME_PARTIAL and not pos['time_partial'] and not pos['partial_closed']:
                if bar_age >= config.TIME_PARTIAL_BARS and profit_atr >= config.TIME_PARTIAL_PROFIT_ATR:
                    self._close_partial(symbol, pos['size'] * config.PARTIAL_CLOSE_PCT, price, "time_partial")
                    pos['stop_loss'] = pos['entry_price']
                    pos['time_partial'] = True

            # ---------- Exit checks ----------
            exit_reason = None
            if is_long and signal == 'SELL' and getattr(self, '_prev_sig_' + symbol, None) == 'SELL':
                exit_reason = "signal_flip"
            elif not is_long and signal == 'BUY' and getattr(self, '_prev_sig_' + symbol, None) == 'BUY':
                exit_reason = "signal_flip"

            if exit_reason is None and config.SMA_EXIT_ENABLED and len(df) >= config.SMA_EXIT_CONFIRM_BARS:
                buf = config.SMA_EXIT_BUFFER_ATR
                confirmed = True
                for j in range(len(df) - config.SMA_EXIT_CONFIRM_BARS, len(df)):
                    r = df.iloc[j]
                    if is_long and not (r['close'] < r['sma_200'] - buf * r['atr_14']):
                        confirmed = False
                        break
                    if not is_long and not (r['close'] > r['sma_200'] + buf * r['atr_14']):
                        confirmed = False
                        break
                if confirmed:
                    exit_reason = "sma_cross"

            if exit_reason is None and bar_age >= config.TIME_LIMIT_BARS:
                exit_reason = "time_limit"

            if is_long:
                if pos['retracement_activated'] and pos['retracement_stop'] and price <= pos['retracement_stop']:
                    exit_reason = "profit_lock"
                elif price <= pos['stop_loss']:
                    exit_reason = "stop_loss"
                elif price >= pos['take_profit']:
                    exit_reason = "take_profit"
            else:
                if pos['retracement_activated'] and pos['retracement_stop'] and price >= pos['retracement_stop']:
                    exit_reason = "profit_lock"
                elif price >= pos['stop_loss']:
                    exit_reason = "stop_loss"
                elif price <= pos['take_profit']:
                    exit_reason = "take_profit"

            if exit_reason:
                self.flatten_symbol(symbol, price, t, reason=exit_reason)

            setattr(self, '_prev_sig_' + symbol, signal)

    # ------------------------------------------------------------------
    def _maybe_send_eod(self, market_open):
        if self.market_was_open and not market_open and config.TELEGRAM_EOD_REPORT:
            equity = self.get_current_equity()
            reporter.send_eod_report(self.day_trades, equity, self.day_start_equity,
                                     open_positions=self.positions)
        self.market_was_open = market_open

    # ------------------------------------------------------------------
    def run(self):
        stock_syms = [s for s in self.symbols if not self._is_crypto(s)]
        crypto_syms = [s for s in self.symbols if self._is_crypto(s)]
        logger.info(f"LiveTraderMulti v3 | stocks={stock_syms} crypto={crypto_syms}")
        send_telegram(f"🧠 Nexus Core v3 online.\nStocks: {', '.join(stock_syms) or '-'}\n"
                      f"Crypto: {', '.join(crypto_syms) or '-'}\nMode: {config.UNIVERSE_MODE}", kind='brain')

        while self.running:
            try:
                market_open = self.is_market_open()
                self._maybe_send_eod(market_open)

                # Crypto trades 24/7; stocks only in market hours
                active = list(crypto_syms)
                if market_open:
                    active += stock_syms
                if not active:
                    logger.info("Market closed, no crypto - sleeping 5 min")
                    time.sleep(300)
                    continue

                self.cycle_count += 1
                if self.cycle_count % config.TELEGRAM_HEARTBEAT_CYCLES == 0:
                    equity = self.get_current_equity()
                    open_n = sum(1 for p in self.positions.values() if p is not None)
                    send_telegram(reporter.format_capital_update(
                        equity, self.day_start_equity, self.peak_equity, open_n), kind='heartbeat')

                for sym in active:
                    self.process_symbol(sym)

                time.sleep(config.BAR_MINUTES * 60)
            except KeyboardInterrupt:
                logger.info("Shutdown requested.")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}", exc_info=True)
                send_telegram(f"Live trader error: {e}", kind='critical')
                time.sleep(60)

        if self.journal:
            pd.DataFrame(self.journal).to_csv("logs/trade_journal.csv", index=False)


if __name__ == "__main__":
    LiveTraderMulti().run()
