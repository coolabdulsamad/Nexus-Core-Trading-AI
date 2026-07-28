#!/usr/bin/env python3
"""
src/live/live_paper_trader_multi.py
Multi‑symbol live trader – STABILISED VERSION
- Market orders only (no cancellations)
- Uses fully closed 5‑min bars only
- Requires two consecutive exit signals before closing
- Loop every 5 minutes (matches bar frequency)
- Simplified exit logic (SL/TP, breakeven, trailing stop only)
"""
import os
import sys
import time
import requests
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from requests.exceptions import ConnectionError, Timeout

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.common.exceptions import APIError

sys.path.insert(0, '/opt/nexus_core')
from config.settings import config
from src.models.meta_learner import MetaLearner
from src.backtester.risk_gate import RiskGate
from src.ingestion.indicator_calculator import calculate_all_indicators
from utils.logger import setup_logger
from utils.telegram import send_telegram

logger = setup_logger("LiveTraderMulti", "logs/live_trader_multi.log")

# --- Configuration ---
COOLDOWN_BARS = 2
TIME_LIMIT_BARS = 48                    # allow more time for trend to develop
BASE_ENTRY_CONFIDENCE = config.BASE_ENTRY_CONFIDENCE
ENTRY_CONVICTION_MARGIN = config.ENTRY_CONVICTION_MARGIN
MAX_RETRIES = 3
RETRY_DELAY = 5
DATA_FETCH_TIMEOUT = config.DATA_FETCH_TIMEOUT
MAX_DRAWDOWN_PCT = config.MAX_DRAWDOWN_PCT
USE_BROKER_BRACKET = config.USE_BROKER_BRACKET_ORDERS

# --- Volatility filter threshold (minimum ATR) ---
MIN_ATR_THRESHOLD = 0.5

# --- Profit-locking / exit management (from config) ---
ENABLE_PARTIAL_TAKE_PROFIT = config.ENABLE_PARTIAL_TAKE_PROFIT
PARTIAL_TP_THRESHOLD = config.PARTIAL_TP_THRESHOLD
PARTIAL_CLOSE_PCT = config.PARTIAL_CLOSE_PCT
ENABLE_TRAILING_TP = config.ENABLE_TRAILING_TP
TRAILING_TP_ATR_TRIGGER = config.TRAILING_TP_ATR_TRIGGER
TRAILING_TP_DISTANCE_ATR = config.TRAILING_TP_DISTANCE_ATR
ENABLE_PROFIT_DRAWDOWN_PROTECTION = config.ENABLE_PROFIT_DRAWDOWN_PROTECTION
RETRACEMENT_HIGH_THRESHOLD = config.RETRACEMENT_HIGH_THRESHOLD
RETRACEMENT_LOCK_THRESHOLD = config.RETRACEMENT_LOCK_THRESHOLD
ENABLE_TIME_PARTIAL = config.ENABLE_TIME_PARTIAL
TIME_PARTIAL_BARS = config.TIME_PARTIAL_BARS
TIME_PARTIAL_PROFIT_ATR = config.TIME_PARTIAL_PROFIT_ATR


def _new_position(pos_type, entry_price, size, sl, tp, entry_time, atr):
    """Build a fresh position dict with all profit-lock bookkeeping fields."""
    return {
        'type': pos_type,
        'entry_price': entry_price,
        'size': size,
        'stop_loss': sl,
        'take_profit': tp,
        'entry_time': entry_time,
        'highest_price': entry_price,
        'lowest_price': entry_price,
        'atr': atr,
        'partial_closed': False,
        'trailing_tp_activated': False,
        'trailing_tp_distance': None,
        'time_partial': False,
        'retracement_activated': False,
        'retracement_stop': None,
    }

# --- Backup data (Yahoo Finance) ---
USE_YAHOO_FALLBACK = True
try:
    import yfinance as yf
except ImportError:
    yf = None
    logger.warning("yfinance not installed. Yahoo fallback disabled.")
    USE_YAHOO_FALLBACK = False

# --- Order polling ---
ORDER_POLL_TIMEOUT = 30

class LiveTraderMulti:
    def __init__(self, symbols=None, initial_capital=100000):
        if symbols is None:
            symbols = ["AAPL", "TSLA", "MSFT", "GOOGL", "NVDA"]
        self.symbols = symbols
        self.initial_capital = initial_capital
        self.capital_per_symbol = initial_capital / len(symbols)

        self.meta_learner = MetaLearner()
        self.trading_client = TradingClient(
            api_key=os.getenv("ALPACA_API_KEY"),
            secret_key=os.getenv("ALPACA_SECRET_KEY"),
            paper=True
        )
        self.data_client = StockHistoricalDataClient(
            api_key=os.getenv("ALPACA_API_KEY"),
            secret_key=os.getenv("ALPACA_SECRET_KEY")
        )

        self.risk_gates = {sym: RiskGate(self.capital_per_symbol) for sym in symbols}
        self.positions = {sym: None for sym in symbols}
        self.cooldown = {sym: 0 for sym in symbols}
        self.pending_exit = {sym: False for sym in symbols}
        self.exit_reason = {sym: "" for sym in symbols}
        self.running = True
        self.day_start_equity = self.get_current_equity()
        self.cycle_count = 0
        self.peak_equity = self.day_start_equity
        self.last_date = None

        # For exit signal hysteresis
        self.prev_signal = {sym: None for sym in symbols}

        # Trade journal
        self.journal = []

        # Startup cleanup
        logger.info("Cleaning up open orders from previous runs...")
        for sym in self.symbols:
            self.cancel_open_orders(sym)

        self.reconcile_all_positions()

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def _retry_call(self, func, *args, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except (ConnectionError, Timeout, APIError) as e:
                logger.warning(f"API call failed (attempt {attempt+1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    raise
        return None

    def get_current_equity(self):
        try:
            account = self._retry_call(self.trading_client.get_account)
            return float(account.equity)
        except Exception as e:
            logger.error(f"Failed to get account: {e}")
            return sum(self.risk_gates[s].capital for s in self.symbols)

    def get_account_buying_power(self):
        try:
            account = self._retry_call(self.trading_client.get_account)
            return float(account.buying_power)
        except Exception as e:
            logger.error(f"Failed to get buying power: {e}")
            return 0

    def get_position(self, symbol):
        try:
            if hasattr(self.trading_client, 'get_position'):
                return self._retry_call(self.trading_client.get_position, symbol)
            else:
                positions = self._retry_call(self.trading_client.get_all_positions)
                for pos in positions:
                    if pos.symbol == symbol:
                        return pos
                return None
        except APIError as e:
            if "position does not exist" in str(e):
                return None
            logger.error(f"Error fetching position for {symbol}: {e}")
            return None

    def side_str(self, pos):
        if pos is None:
            return None
        side = pos.side
        if hasattr(side, 'value'):
            return side.value
        return str(side).lower()

    def cancel_open_orders(self, symbol):
        try:
            orders = self._retry_call(self.trading_client.get_orders)
            if not orders:
                return
            api_key = os.getenv("ALPACA_API_KEY")
            secret_key = os.getenv("ALPACA_SECRET_KEY")
            base_url = "https://paper-api.alpaca.markets"
            for order in orders:
                if order.symbol != symbol:
                    continue
                order_id = order.id
                cancelled = False
                for method_name in ['cancel_order', 'cancel_order_by_id', 'delete_order']:
                    if hasattr(self.trading_client, method_name):
                        try:
                            self._retry_call(getattr(self.trading_client, method_name), order_id)
                            logger.info(f"Cancelled order {order_id} for {symbol} via SDK method {method_name}")
                            cancelled = True
                            break
                        except Exception as e:
                            logger.warning(f"SDK method {method_name} failed: {e}")
                            continue
                if not cancelled:
                    url = f"{base_url}/v2/orders/{order_id}"
                    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret_key}
                    try:
                        resp = requests.delete(url, headers=headers)
                        if resp.status_code in (200, 204):
                            logger.info(f"Cancelled order {order_id} for {symbol} via REST DELETE")
                            cancelled = True
                        else:
                            logger.error(f"REST DELETE failed: {resp.status_code} - {resp.text}")
                    except Exception as e:
                        logger.error(f"REST DELETE exception: {e}")
                if not cancelled:
                    logger.error(f"Could not cancel order {order_id} for {symbol}")
        except Exception as e:
            logger.error(f"Failed to cancel orders for {symbol}: {e}")

    def submit_order_with_retry(self, order):
        return self._retry_call(self.trading_client.submit_order, order)

    # ------------------------------------------------------------------
    # Data fetching – only closed 5-min bars
    # ------------------------------------------------------------------
    def get_bars_for_symbol(self, symbol):
        end = datetime.now()
        start = end - timedelta(days=7)

        def _fetch_alpaca():
            feeds = ['sip', 'iex']
            for feed in feeds:
                try:
                    request = StockBarsRequest(
                        symbol_or_symbols=[symbol],
                        timeframe=TimeFrame.Minute,
                        start=start,
                        end=end,
                        limit=10000,
                        feed=feed
                    )
                    bars = self._retry_call(self.data_client.get_stock_bars, request).data
                    if symbol in bars and len(bars[symbol]) > 0:
                        df = pd.DataFrame([{
                            'timestamp': bar.timestamp,
                            'open': bar.open,
                            'high': bar.high,
                            'low': bar.low,
                            'close': bar.close,
                            'volume': bar.volume
                        } for bar in bars[symbol]])
                        df = df.sort_values('timestamp').reset_index(drop=True)
                        df = df.set_index('timestamp')
                        df_5min = df.resample('5min').agg({
                            'open': 'first',
                            'high': 'max',
                            'low': 'min',
                            'close': 'last',
                            'volume': 'sum'
                        }).dropna().reset_index()
                        # ---- CRITICAL: Drop incomplete bar ----
                        if not df_5min.empty:
                            last_time = df_5min.iloc[-1]['timestamp']
                            if pd.Timestamp.now(tz='UTC') - last_time < pd.Timedelta(minutes=5):
                                df_5min = df_5min.iloc[:-1]  # remove open bar
                        return df_5min
                except Exception as e:
                    logger.warning(f"Feed {feed} failed for {symbol}: {e}")
                    continue
            return pd.DataFrame()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_fetch_alpaca)
            try:
                df = future.result(timeout=DATA_FETCH_TIMEOUT)
                if not df.empty:
                    latest = df.iloc[-1]['timestamp']
                    logger.info(f"{symbol}: {len(df)} closed bars (Alpaca), latest = {latest}")
                    return df
            except concurrent.futures.TimeoutError:
                logger.error(f"{symbol}: Alpaca data fetch timed out after {DATA_FETCH_TIMEOUT}s")
            except Exception as e:
                logger.error(f"{symbol}: Alpaca data fetch error: {e}")

        if USE_YAHOO_FALLBACK and yf is not None:
            try:
                logger.info(f"{symbol}: using Yahoo Finance fallback")
                ticker = yf.Ticker(symbol)
                df_yf = ticker.history(period="5d", interval="5m")
                if not df_yf.empty:
                    df_yf = df_yf.reset_index()
                    df_yf['timestamp'] = df_yf['Datetime'].dt.tz_localize('UTC')
                    df_yf = df_yf.rename(columns={
                        'Open': 'open',
                        'High': 'high',
                        'Low': 'low',
                        'Close': 'close',
                        'Volume': 'volume'
                    })
                    df_yf = df_yf[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                    # Also drop incomplete bar for Yahoo
                    if not df_yf.empty:
                        last_time = df_yf.iloc[-1]['timestamp']
                        if pd.Timestamp.now(tz='UTC') - last_time < pd.Timedelta(minutes=5):
                            df_yf = df_yf.iloc[:-1]
                    logger.info(f"{symbol}: {len(df_yf)} closed bars (Yahoo), latest = {df_yf.iloc[-1]['timestamp'] if not df_yf.empty else 'None'}")
                    return df_yf
            except Exception as e:
                logger.error(f"{symbol}: Yahoo fallback failed: {e}")

        return pd.DataFrame()

    def is_market_open(self):
        try:
            clock = self._retry_call(self.trading_client.get_clock)
            return clock.is_open
        except Exception as e:
            logger.warning(f"Could not check market hours: {e}. Assuming closed.")
            return False

    # ------------------------------------------------------------------
    # Reconciliation & helpers
    # ------------------------------------------------------------------
    def reconcile_all_positions(self):
        for sym in self.symbols:
            broker_pos = self.get_position(sym)
            if broker_pos is None:
                if self.positions[sym] is not None:
                    logger.info(f"{sym}: position closed externally – clearing local.")
                    if self.exit_reason[sym]:
                        self.log_trade({
                            'symbol': sym,
                            'exit_reason': self.exit_reason[sym],
                            'exit_time': datetime.now(),
                            'final_size': 0
                        })
                        self.exit_reason[sym] = ""
                    self.positions[sym] = None
                    self.pending_exit[sym] = False
                continue
            side = self.side_str(broker_pos)
            qty = float(broker_pos.qty)
            entry = float(broker_pos.avg_entry_price)
            if self.positions[sym] is None:
                self.positions[sym] = _new_position(
                    'LONG' if side == 'long' else 'SHORT',
                    entry, qty, 0.0, 0.0, pd.Timestamp.now(tz='UTC'), None
                )
                logger.info(f"{sym}: reconciled existing position {side} {qty} @ {entry}")
            else:
                if abs(self.positions[sym]['size'] - qty) > 0.01:
                    logger.info(f"{sym}: syncing position size from {self.positions[sym]['size']} to {qty}")
                    self.positions[sym]['size'] = qty
                if self.positions[sym]['type'].lower() != side:
                    logger.warning(f"{sym}: side mismatch – updating from {self.positions[sym]['type']} to {side}")
                    self.positions[sym]['type'] = 'LONG' if side == 'long' else 'SHORT'
                    self.positions[sym]['entry_price'] = entry

    def daily_resync_if_needed(self):
        today = datetime.now().date()
        if self.last_date is None:
            self.last_date = today
            return
        if today != self.last_date:
            logger.info(f"New day detected ({today}). Performing full reconciliation.")
            self.reconcile_all_positions()
            self.last_date = today

    def log_trade(self, trade_data):
        self.journal.append(trade_data)
        if len(self.journal) % 10 == 0:
            df = pd.DataFrame(self.journal)
            df.to_csv("logs/trade_journal.csv", index=False)

    # ------------------------------------------------------------------
    # Flatten symbol
    # ------------------------------------------------------------------
    def _poll_position_closed(self, symbol, timeout=ORDER_POLL_TIMEOUT):
        start = time.time()
        while time.time() - start < timeout:
            pos = self.get_position(symbol)
            if pos is None:
                return True
            time.sleep(1)
        return False

    def flatten_symbol(self, symbol, current_price, current_time, reason="manual"):
        self.cancel_open_orders(symbol)
        broker_pos = self.get_position(symbol)
        if broker_pos is None:
            logger.info(f"{symbol}: no position to flatten.")
            self.positions[symbol] = None
            self.pending_exit[symbol] = False
            self.exit_reason[symbol] = ""
            return

        live_qty = abs(float(broker_pos.qty))
        side = self.side_str(broker_pos)
        if live_qty <= 0:
            logger.info(f"{symbol}: position qty is zero, clearing local.")
            self.positions[symbol] = None
            self.pending_exit[symbol] = False
            self.exit_reason[symbol] = ""
            self.cooldown[symbol] = COOLDOWN_BARS
            return

        logger.info(f"{symbol}: attempting to flatten {side} position of {live_qty} shares. Reason: {reason}")
        self.exit_reason[symbol] = reason
        pos = self.positions.get(symbol)

        for attempt in range(MAX_RETRIES):
            try:
                close_resp = self._retry_call(self.trading_client.close_position, symbol)
                logger.info(f"{symbol}: close_position() submitted successfully. Response: {close_resp}")
                self.pending_exit[symbol] = True

                if self._poll_position_closed(symbol):
                    logger.info(f"{symbol}: position closed successfully.")
                    if pos:
                        entry_price = pos.get('entry_price', current_price)
                        if side == 'long':
                            pnl = (current_price - entry_price) * live_qty
                        else:
                            pnl = (entry_price - current_price) * live_qty
                        pnl_pct = (pnl / (entry_price * live_qty)) * 100 if entry_price * live_qty != 0 else 0
                        msg = (f"✅ EXIT {symbol} ({reason})\n"
                               f"Side: {side.upper()} | Qty: {live_qty}\n"
                               f"Entry: ${entry_price:.2f} | Exit: ${current_price:.2f}\n"
                               f"PnL: ${pnl:.2f} ({pnl_pct:+.2f}%)")
                        send_telegram(msg)
                    else:
                        send_telegram(f"✅ EXIT {symbol} ({reason}) – position closed (PnL unknown)")
                    self.positions[symbol] = None
                    self.pending_exit[symbol] = False
                    self.cooldown[symbol] = COOLDOWN_BARS
                    self.exit_reason[symbol] = ""
                    return
                else:
                    logger.warning(f"{symbol}: position still open after polling. May need manual check.")
                    return
            except Exception as e:
                logger.error(f"{symbol}: close_position attempt {attempt+1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    logger.warning(f"{symbol}: fallback to manual market order for closing.")
                    self._close_with_market_order(symbol, live_qty, side)
                    return

    def _close_with_market_order(self, symbol, qty, side):
        order_side = OrderSide.SELL if side == 'long' else OrderSide.BUY
        try:
            order = MarketOrderRequest(symbol=symbol, qty=qty, side=order_side, time_in_force=TimeInForce.DAY)
            submitted = self.submit_order_with_retry(order)
            if submitted and hasattr(submitted, 'id'):
                logger.info(f"{symbol}: fallback market exit order {submitted.id} submitted.")
                self.pending_exit[symbol] = True
                if self._poll_position_closed(symbol):
                    logger.info(f"{symbol}: position closed via fallback.")
                    self.positions[symbol] = None
                    self.pending_exit[symbol] = False
                    self.cooldown[symbol] = COOLDOWN_BARS
                    self.exit_reason[symbol] = ""
                    send_telegram(f"✅ EXIT {symbol} (fallback) – position closed.")
                else:
                    logger.warning(f"{symbol}: fallback order did not fill within timeout.")
            else:
                logger.error(f"{symbol}: fallback market exit order failed.")
                self.pending_exit[symbol] = False
                send_telegram(f"🚨 CRITICAL: Failed to close {symbol} position after all attempts.")
        except Exception as e:
            logger.error(f"{symbol}: fallback market exit error: {e}")
            self.pending_exit[symbol] = False
            send_telegram(f"🚨 CRITICAL: Error closing {symbol}: {e}")

    # ------------------------------------------------------------------
    # Partial close (scale out to lock profit)
    # ------------------------------------------------------------------
    def _close_partial(self, symbol, qty, price, reason):
        qty = int(round(qty))
        if qty <= 0:
            return
        broker_pos = self.get_position(symbol)
        if broker_pos is None:
            logger.warning(f"{symbol}: cannot partial-close – no position.")
            return
        side = self.side_str(broker_pos)
        live_qty = abs(float(broker_pos.qty))
        if qty >= live_qty:
            logger.info(f"{symbol}: partial qty {qty} >= live {live_qty}; flattening instead.")
            self.flatten_symbol(symbol, price, price, reason=reason)
            return
        order_side = OrderSide.SELL if side == 'long' else OrderSide.BUY
        try:
            order = MarketOrderRequest(symbol=symbol, qty=qty, side=order_side, time_in_force=TimeInForce.DAY)
            submitted = self.submit_order_with_retry(order)
            if submitted and hasattr(submitted, 'id'):
                logger.info(f"{symbol}: partial close of {qty} shares submitted ({reason})")
                send_telegram(f"🔹 PARTIAL EXIT {symbol} ({reason}): {qty} shares @ ${price:.2f}")
                time.sleep(2)
                new_pos = self.get_position(symbol)
                if new_pos is None:
                    self.positions[symbol] = None
                    self.cooldown[symbol] = COOLDOWN_BARS
                elif self.positions[symbol] is not None:
                    self.positions[symbol]['size'] = abs(float(new_pos.qty))
                    logger.info(f"{symbol}: remaining size = {self.positions[symbol]['size']}")
            else:
                logger.error(f"{symbol}: partial close order failed.")
        except Exception as e:
            logger.error(f"{symbol}: partial close error: {e}")

    # ------------------------------------------------------------------
    # Core logic per symbol (profit-locking exits)
    # ------------------------------------------------------------------
    def process_symbol(self, symbol):
        self.daily_resync_if_needed()

        if self.pending_exit[symbol]:
            broker_pos = self.get_position(symbol)
            if broker_pos is None:
                logger.info(f"{symbol}: pending exit cleared – position closed.")
                self.pending_exit[symbol] = False
                self.positions[symbol] = None
                self.cooldown[symbol] = COOLDOWN_BARS
                self.exit_reason[symbol] = ""
                return
            else:
                logger.debug(f"{symbol}: exit order still pending...")
                return

        if self.cooldown[symbol] > 0:
            self.cooldown[symbol] -= 1
            logger.info(f"{symbol}: cooldown {self.cooldown[symbol]} bars remaining")
            return

        self.reconcile_all_positions()

        df = self.get_bars_for_symbol(symbol)
        if df.empty:
            logger.warning(f"{symbol}: no closed data")
            return

        latest_time = df.iloc[-1]['timestamp']
        age_seconds = (datetime.now(latest_time.tzinfo) - latest_time).total_seconds()
        if age_seconds > 7200:
            logger.critical(f"{symbol}: data is stale (age {age_seconds/60:.1f} min). Skipping.")
            send_telegram(f"⚠️ {symbol} data stale ({age_seconds/60:.0f} min old)")
            return

        df = calculate_all_indicators(df)
        df['sma_200'] = df['close'].rolling(window=200).mean()
        df = df.dropna(subset=['sma_200'])

        if len(df) == 0:
            logger.info(f"{symbol}: not enough bars yet")
            return

        row = df.iloc[-1]
        current_price = row['close']
        current_time = row['timestamp']
        atr = row['atr_14']
        sma200 = row['sma_200']

        # --- Volatility filter ---
        if config.VOLATILITY_FILTER_ENABLED and atr < MIN_ATR_THRESHOLD:
            logger.info(f"{symbol}: ATR {atr:.2f} below threshold {MIN_ATR_THRESHOLD}, skipping entry")
            # but we still need to manage positions

        total_equity = self.get_current_equity()
        self.peak_equity = max(self.peak_equity, total_equity)
        drawdown = (self.peak_equity - total_equity) / self.peak_equity if self.peak_equity > 0 else 0
        if drawdown >= MAX_DRAWDOWN_PCT:
            logger.critical(f"⚠️ Max drawdown reached ({drawdown:.2%}) – flattening all positions.")
            send_telegram(f"⚠️ Max drawdown {drawdown:.2%} – flattening all")
            for sym in self.symbols:
                self.flatten_symbol(sym, current_price, current_time, reason="max_drawdown")
            return

        self.risk_gates[symbol].capital = total_equity / len(self.symbols)
        self.risk_gates[symbol].reset_daily_loss_if_new_day(current_time)
        self.risk_gates[symbol].update_unrealized_loss(total_equity)

        if not self.risk_gates[symbol].check_daily_loss_limit(total_equity):
            logger.warning(f"Daily loss limit reached for {symbol}. Flattening.")
            self.flatten_symbol(symbol, current_price, current_time, reason="daily_loss_limit")
            return

        result = self.meta_learner.get_signal(symbol, row, timestamp=current_time)
        signal = result['signal']
        confidence = result['confidence']
        logger.info(f"{symbol} | {current_time} | Price: {current_price:.2f} | Signal: {signal} | Conf: {confidence:.4f}")

        buy_condition = current_price > sma200
        sell_condition = current_price < sma200
        logger.info(f"{symbol}: DEBUG - SMA200={sma200:.2f}, price={current_price:.2f}, buy_ok={buy_condition}, sell_ok={sell_condition}")

        is_volatility_ok = (atr >= MIN_ATR_THRESHOLD) if config.VOLATILITY_FILTER_ENABLED else True
        # CONVICTION, not raw probability. MetaLearner encodes direction in the
        # probability (BUY>0.5, SELL<0.5), so a SELL's confidence is ALWAYS <0.5.
        # Gating on `confidence >= 0.5` made every short impossible (the reason it
        # was not trading). Gate on distance from 0.5 instead.
        conviction = abs(confidence - 0.5)
        confidence_ok = conviction >= ENTRY_CONVICTION_MARGIN
        logger.info(f"{symbol}: conviction={conviction:.4f} (margin {ENTRY_CONVICTION_MARGIN}), "
                    f"conf_ok={confidence_ok}, vol_ok={is_volatility_ok}")

        broker_pos = self.get_position(symbol)
        broker_side = self.side_str(broker_pos)

        # ---------- Entry logic (flat) ----------
        if broker_pos is None:
            self.cancel_open_orders(symbol)

            if is_volatility_ok and confidence_ok:
                if signal == 'BUY' and buy_condition:
                    logger.info(f"{symbol}: BUY entry condition met (price > SMA200), attempting to enter.")
                    size, sl, tp = self.risk_gates[symbol].calculate_long_position_size(current_price, atr)
                    if size > 0:
                        qty = round(size, 0)
                        if qty <= 0:
                            return
                        cost = qty * current_price
                        buying_power = self.get_account_buying_power()
                        if cost > buying_power:
                            logger.warning(f"{symbol}: insufficient buying power (need ${cost:.2f}, have ${buying_power:.2f})")
                            return
                        # Use market order (no limit)
                        order = MarketOrderRequest(symbol=symbol, qty=qty, side=OrderSide.BUY, time_in_force=TimeInForce.DAY)
                        try:
                            submitted = self.submit_order_with_retry(order)
                            if submitted and hasattr(submitted, 'id'):
                                self.positions[symbol] = _new_position(
                                    'LONG', current_price, size, sl, tp, current_time, atr
                                )
                                msg = f"LONG ENTRY {symbol}: {qty} shares @ {current_price:.2f}, SL: {sl:.2f}, TP: {tp:.2f}"
                                logger.info(msg)
                                send_telegram(msg)
                            else:
                                logger.error(f"{symbol}: entry order submission failed.")
                        except Exception as e:
                            logger.error(f"Order failed: {e}")

                elif signal == 'SELL' and sell_condition:
                    logger.info(f"{symbol}: SELL entry condition met (price < SMA200), attempting to enter.")
                    size, sl, tp = self.risk_gates[symbol].calculate_short_position_size(current_price, atr)
                    if size > 0:
                        qty = round(size, 0)
                        if qty <= 0:
                            return
                        cost = qty * current_price
                        buying_power = self.get_account_buying_power()
                        if cost > buying_power:
                            logger.warning(f"{symbol}: insufficient buying power (need ${cost:.2f}, have ${buying_power:.2f})")
                            return
                        order = MarketOrderRequest(symbol=symbol, qty=qty, side=OrderSide.SELL, time_in_force=TimeInForce.DAY)
                        try:
                            submitted = self.submit_order_with_retry(order)
                            if submitted and hasattr(submitted, 'id'):
                                self.positions[symbol] = _new_position(
                                    'SHORT', current_price, size, sl, tp, current_time, atr
                                )
                                msg = f"SHORT ENTRY {symbol}: {qty} shares @ {current_price:.2f}, SL: {sl:.2f}, TP: {tp:.2f}"
                                logger.info(msg)
                                send_telegram(msg)
                            else:
                                logger.error(f"{symbol}: entry order submission failed.")
                        except Exception as e:
                            logger.error(f"Order failed: {e}")
                else:
                    logger.info(f"{symbol}: Entry condition not met. Signal: {signal}, buy_ok={buy_condition}, sell_ok={sell_condition}")

        else:
            # ---------- In position – manage exit ----------
            pos = self.positions[symbol]
            if pos is None:
                pos = _new_position(
                    'LONG' if broker_side == 'long' else 'SHORT',
                    float(broker_pos.avg_entry_price), abs(float(broker_pos.qty)),
                    0.0, 0.0, current_time, atr if atr is not None else 0.0
                )
                self.positions[symbol] = pos

            pos_atr = pos.get('atr')
            if pos_atr is None:
                pos_atr = atr if atr is not None else 0.0

            # ---- Update trailing stop and breakeven (simplified) ----
            if pos['type'] == 'LONG':
                pos['highest_price'] = max(pos['highest_price'], current_price)
                profit_atr = (current_price - pos['entry_price']) / pos_atr if pos_atr > 0 else 0

                # Breakeven (2.5×ATR)
                if profit_atr >= 2.5 and pos['stop_loss'] < pos['entry_price']:
                    pos['stop_loss'] = pos['entry_price']
                    logger.info(f"{symbol}: breakeven SL set to {pos['entry_price']:.2f}")

                # Trailing stop (trigger at 4.0 ATR)
                if profit_atr >= 4.0:
                    if profit_atr >= 6.0:
                        trail_dist = 2.5 * pos_atr
                    else:
                        trail_dist = 4.0 * pos_atr
                    new_sl = pos['highest_price'] - trail_dist
                    if new_sl > pos['stop_loss']:
                        pos['stop_loss'] = new_sl
                        logger.info(f"{symbol}: trailing SL updated to {new_sl:.2f}")

                tp_distance = pos['take_profit'] - pos['entry_price']
                profit_to_tp = ((current_price - pos['entry_price']) / tp_distance) if tp_distance > 0 else 0

                # Partial take-profit: bank half once we reach X% of the target
                if ENABLE_PARTIAL_TAKE_PROFIT and not pos['partial_closed'] and tp_distance > 0:
                    if profit_to_tp >= PARTIAL_TP_THRESHOLD:
                        self._close_partial(symbol, pos['size'] * PARTIAL_CLOSE_PCT, current_price, "partial_tp")
                        if pos['stop_loss'] < pos['entry_price']:
                            pos['stop_loss'] = pos['entry_price']   # lock breakeven on the rest
                        pos['partial_closed'] = True

                # Profit-drawdown protection: once profit hits 70% of TP, never give
                # back below 50% of TP profit (this is the "lock in the gain" you wanted).
                if ENABLE_PROFIT_DRAWDOWN_PROTECTION and not pos['retracement_activated'] and tp_distance > 0:
                    if profit_to_tp >= RETRACEMENT_HIGH_THRESHOLD:
                        pos['retracement_stop'] = pos['entry_price'] + tp_distance * RETRACEMENT_LOCK_THRESHOLD
                        pos['retracement_activated'] = True
                        logger.info(f"{symbol}: profit lock armed at {profit_to_tp:.0%} of TP, "
                                    f"floor ${pos['retracement_stop']:.2f}")

                # Trailing take-profit: let a strong winner run, trailing the TP up
                if ENABLE_TRAILING_TP and profit_atr >= TRAILING_TP_ATR_TRIGGER:
                    if not pos['trailing_tp_activated']:
                        pos['trailing_tp_activated'] = True
                        pos['trailing_tp_distance'] = TRAILING_TP_DISTANCE_ATR * pos_atr
                    new_tp = pos['highest_price'] - pos['trailing_tp_distance']
                    if new_tp > pos['take_profit']:
                        pos['take_profit'] = new_tp

                # Time-based partial: stalled but green after TIME_PARTIAL_BARS
                if ENABLE_TIME_PARTIAL and not pos['time_partial'] and not pos['partial_closed']:
                    bar_age = (current_time - pos['entry_time']).total_seconds() / 300
                    if bar_age >= TIME_PARTIAL_BARS and profit_atr >= TIME_PARTIAL_PROFIT_ATR:
                        self._close_partial(symbol, pos['size'] * PARTIAL_CLOSE_PCT, current_price, "time_partial")
                        if pos['stop_loss'] < pos['entry_price']:
                            pos['stop_loss'] = pos['entry_price']
                        pos['time_partial'] = True

            else:  # SHORT
                pos['lowest_price'] = min(pos['lowest_price'], current_price)
                profit_atr = (pos['entry_price'] - current_price) / pos_atr if pos_atr > 0 else 0

                if profit_atr >= 2.5 and pos['stop_loss'] > pos['entry_price']:
                    pos['stop_loss'] = pos['entry_price']
                    logger.info(f"{symbol}: breakeven SL set to {pos['entry_price']:.2f}")

                if profit_atr >= 4.0:
                    if profit_atr >= 6.0:
                        trail_dist = 2.5 * pos_atr
                    else:
                        trail_dist = 4.0 * pos_atr
                    new_sl = pos['lowest_price'] + trail_dist
                    if new_sl < pos['stop_loss']:
                        pos['stop_loss'] = new_sl
                        logger.info(f"{symbol}: trailing SL updated to {new_sl:.2f}")

                tp_distance = pos['entry_price'] - pos['take_profit']
                profit_to_tp = ((pos['entry_price'] - current_price) / tp_distance) if tp_distance > 0 else 0

                # Partial take-profit
                if ENABLE_PARTIAL_TAKE_PROFIT and not pos['partial_closed'] and tp_distance > 0:
                    if profit_to_tp >= PARTIAL_TP_THRESHOLD:
                        self._close_partial(symbol, pos['size'] * PARTIAL_CLOSE_PCT, current_price, "partial_tp")
                        if pos['stop_loss'] > pos['entry_price']:
                            pos['stop_loss'] = pos['entry_price']
                        pos['partial_closed'] = True

                # Profit-drawdown protection (lock the gain)
                if ENABLE_PROFIT_DRAWDOWN_PROTECTION and not pos['retracement_activated'] and tp_distance > 0:
                    if profit_to_tp >= RETRACEMENT_HIGH_THRESHOLD:
                        pos['retracement_stop'] = pos['entry_price'] - tp_distance * RETRACEMENT_LOCK_THRESHOLD
                        pos['retracement_activated'] = True
                        logger.info(f"{symbol}: profit lock armed at {profit_to_tp:.0%} of TP, "
                                    f"floor ${pos['retracement_stop']:.2f}")

                # Trailing take-profit
                if ENABLE_TRAILING_TP and profit_atr >= TRAILING_TP_ATR_TRIGGER:
                    if not pos['trailing_tp_activated']:
                        pos['trailing_tp_activated'] = True
                        pos['trailing_tp_distance'] = TRAILING_TP_DISTANCE_ATR * pos_atr
                    new_tp = pos['lowest_price'] + pos['trailing_tp_distance']
                    if new_tp < pos['take_profit']:
                        pos['take_profit'] = new_tp

                # Time-based partial
                if ENABLE_TIME_PARTIAL and not pos['time_partial'] and not pos['partial_closed']:
                    bar_age = (current_time - pos['entry_time']).total_seconds() / 300
                    if bar_age >= TIME_PARTIAL_BARS and profit_atr >= TIME_PARTIAL_PROFIT_ATR:
                        self._close_partial(symbol, pos['size'] * PARTIAL_CLOSE_PCT, current_price, "time_partial")
                        if pos['stop_loss'] > pos['entry_price']:
                            pos['stop_loss'] = pos['entry_price']
                        pos['time_partial'] = True

            # ---- Exit checks with hysteresis ----
            exit_signal = False
            exit_reason = ""

            # Store current signal for next cycle
            current_signal = signal

            # For LONG: exit if signal is SELL and previous signal was also SELL (two consecutive)
            if pos['type'] == 'LONG':
                if signal == 'SELL' and self.prev_signal[symbol] == 'SELL':
                    exit_signal = True
                    exit_reason = "signal_flip"
                # Also exit on SMA cross (with hysteresis: require two consecutive closes below SMA)
                if current_price < sma200 and self.prev_signal[symbol] in ('SELL', 'HOLD'):
                    # We'll track a separate condition? For simplicity, we'll also allow exit on SMA cross if signal is not BUY
                    # But to add hysteresis, we can check if previous bar also below SMA.
                    # We'll compute previous bar's SMA if available.
                    if len(df) >= 2:
                        prev_row = df.iloc[-2]
                        prev_price = prev_row['close']
                        prev_sma = prev_row['sma_200']
                        if prev_price < prev_sma and current_price < sma200:
                            exit_signal = True
                            exit_reason = "sma_cross"
                    else:
                        # If not enough data, just exit on single cross (less strict)
                        if current_price < sma200:
                            exit_signal = True
                            exit_reason = "sma_cross"
            else:  # SHORT
                if signal == 'BUY' and self.prev_signal[symbol] == 'BUY':
                    exit_signal = True
                    exit_reason = "signal_flip"
                if current_price > sma200:
                    if len(df) >= 2:
                        prev_row = df.iloc[-2]
                        prev_price = prev_row['close']
                        prev_sma = prev_row['sma_200']
                        if prev_price > prev_sma and current_price > sma200:
                            exit_signal = True
                            exit_reason = "sma_cross"
                    else:
                        if current_price > sma200:
                            exit_signal = True
                            exit_reason = "sma_cross"

            # Time limit (full exit)
            bar_age = (current_time - pos['entry_time']).total_seconds() / 300
            if bar_age >= TIME_LIMIT_BARS:
                exit_signal = True
                exit_reason = "time_limit"

            # Stop loss / take profit / profit-lock floor (real exits)
            if pos['type'] == 'LONG':
                if pos['retracement_activated'] and pos['retracement_stop'] is not None \
                        and current_price <= pos['retracement_stop']:
                    exit_signal = True
                    exit_reason = "profit_lock"
                elif current_price <= pos['stop_loss']:
                    exit_signal = True
                    exit_reason = "stop_loss"
                elif current_price >= pos['take_profit']:
                    exit_signal = True
                    exit_reason = "take_profit"
            else:  # SHORT
                if pos['retracement_activated'] and pos['retracement_stop'] is not None \
                        and current_price >= pos['retracement_stop']:
                    exit_signal = True
                    exit_reason = "profit_lock"
                elif current_price >= pos['stop_loss']:
                    exit_signal = True
                    exit_reason = "stop_loss"
                elif current_price <= pos['take_profit']:
                    exit_signal = True
                    exit_reason = "take_profit"

            if exit_signal:
                logger.info(f"{symbol}: exit signal ({exit_reason}) – flattening.")
                self.flatten_symbol(symbol, current_price, current_time, reason=exit_reason)

            # Update previous signal for next iteration
            self.prev_signal[symbol] = signal

    # ------------------------------------------------------------------
    # Main run loop – runs every 5 minutes
    # ------------------------------------------------------------------
    def run(self):
        logger.info(f"Starting Multi-Symbol Live Trader for: {self.symbols}")
        while self.running:
            try:
                if not self.is_market_open():
                    logger.info("Market is closed – sleeping 5 minutes")
                    time.sleep(300)
                    continue

                self.cycle_count += 1
                if self.cycle_count % 2 == 0:  # heartbeat every 2 cycles (10 min)
                    logger.info(f"🔄 Heartbeat: cycle {self.cycle_count} – trader is alive")

                for sym in self.symbols:
                    self.process_symbol(sym)

                # Sleep 5 minutes (matches bar interval)
                time.sleep(300)
            except KeyboardInterrupt:
                logger.info("Shutting down gracefully...")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                send_telegram(f"Live trader error: {e}")
                time.sleep(300)

        if self.journal:
            pd.DataFrame(self.journal).to_csv("logs/trade_journal.csv", index=False)
            logger.info("Trade journal saved.")

if __name__ == "__main__":
    trader = LiveTraderMulti()
    trader.run()