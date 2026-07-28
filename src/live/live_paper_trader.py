#!/usr/bin/env python3
"""
src/live/live_paper_trader.py
Live paper trading executor using Alpaca.
FINAL CONFIG: Volatility OFF, 200-SMA ON, Thresholds 0.55/0.45.
"""
import os
import time
import sys
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestTradeRequest
from alpaca.data.timeframe import TimeFrame

sys.path.insert(0, '/opt/nexus_core')
from config.settings import config
from src.models.meta_learner import MetaLearner
from src.backtester.risk_gate import RiskGate
from src.ingestion.indicator_calculator import calculate_all_indicators
from utils.logger import setup_logger

logger = setup_logger("LiveTrader", "logs/live_trader.log")

class LiveTrader:
    def __init__(self, symbol="AAPL"):
        self.symbol = symbol
        self.meta_learner = MetaLearner()
        self.risk = RiskGate(initial_capital=100000)  # Will update dynamically
        self.trading_client = TradingClient(
            api_key=os.getenv("ALPACA_API_KEY"),
            secret_key=os.getenv("ALPACA_SECRET_KEY"),
            paper=True
        )
        self.data_client = StockHistoricalDataClient(
            api_key=os.getenv("ALPACA_API_KEY"),
            secret_key=os.getenv("ALPACA_SECRET_KEY")
        )
        self.position = None
        self.running = True

    def get_current_equity(self):
        account = self.trading_client.get_account()
        return float(account.equity)

    def get_latest_bars(self, limit=500):
        request = StockBarsRequest(
            symbol_or_symbols=[self.symbol],
            timeframe=TimeFrame.Minute,
            start=datetime.now() - timedelta(days=5),
            limit=limit
        )
        bars = self.data_client.get_stock_bars(request)
        if self.symbol not in bars.data:
            return pd.DataFrame()
        df = pd.DataFrame([{
            'timestamp': bar.timestamp,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume
        } for bar in bars.data[self.symbol]])
        return df

    def process_bar(self, df):
        row = df.iloc[-1]
        current_price = row['close']
        current_time = row['timestamp']

        # Update risk capital with current equity
        self.risk.capital = self.get_current_equity()

        # Get AI signal
        result = self.meta_learner.get_signal(self.symbol, row, timestamp=current_time)
        signal = result['signal']
        confidence = result['confidence']
        logger.info(f"Bar: {current_time} | Price: {current_price:.2f} | Signal: {signal} | Conf: {confidence:.4f}")

        # Trend Filter (200-SMA)
        if 'sma_200' not in row or pd.isna(row['sma_200']):
            return  # Not enough data

        # Volatility Filter: DISABLED
        is_volatility_ok = True

        # Position Management
        if self.position is None:
            if signal == 'BUY' and row['close'] > row['sma_200'] and is_volatility_ok:
                size, sl, tp = self.risk.calculate_long_position_size(current_price, row['atr_14'])
                if size > 0:
                    order = MarketOrderRequest(
                        symbol=self.symbol,
                        qty=round(size, 0),
                        side=OrderSide.BUY,
                        time_in_force=TimeInForce.DAY
                    )
                    self.trading_client.submit_order(order)
                    self.position = {
                        'type': 'LONG',
                        'entry_price': current_price,
                        'size': size,
                        'stop_loss': sl,
                        'take_profit': tp,
                        'entry_time': current_time
                    }
                    logger.info(f"LONG ENTRY: {size:.2f} shares @ {current_price:.2f}, SL: {sl:.2f}, TP: {tp:.2f}")
            elif signal == 'SELL' and row['close'] < row['sma_200'] and is_volatility_ok:
                size, sl, tp = self.risk.calculate_short_position_size(current_price, row['atr_14'])
                if size > 0:
                    order = MarketOrderRequest(
                        symbol=self.symbol,
                        qty=round(size, 0),
                        side=OrderSide.SELL,
                        time_in_force=TimeInForce.DAY
                    )
                    self.trading_client.submit_order(order)
                    self.position = {
                        'type': 'SHORT',
                        'entry_price': current_price,
                        'size': size,
                        'stop_loss': sl,
                        'take_profit': tp,
                        'entry_time': current_time
                    }
                    logger.info(f"SHORT ENTRY: {size:.2f} shares @ {current_price:.2f}, SL: {sl:.2f}, TP: {tp:.2f}")
        else:
            # In a position – check exit conditions
            exit_signal = False
            if self.position['type'] == 'LONG':
                if signal == 'SELL' or current_price >= self.position['take_profit'] or current_price <= self.position['stop_loss']:
                    exit_signal = True
            elif self.position['type'] == 'SHORT':
                if signal == 'BUY' or current_price <= self.position['take_profit'] or current_price >= self.position['stop_loss']:
                    exit_signal = True

            if exit_signal:
                side = OrderSide.SELL if self.position['type'] == 'LONG' else OrderSide.BUY
                order = MarketOrderRequest(
                    symbol=self.symbol,
                    qty=round(self.position['size'], 0),
                    side=side,
                    time_in_force=TimeInForce.DAY
                )
                self.trading_client.submit_order(order)
                logger.info(f"CLOSED {self.position['type']} at {current_price:.2f}")
                self.position = None

    def run(self):
        logger.info("Starting Live Paper Trader (Volatility OFF, 200-SMA ON)...")
        while self.running:
            try:
                df = self.get_latest_bars()
                if df.empty:
                    time.sleep(60)
                    continue

                # Resample to 5-min bars
                df = df.set_index('timestamp')
                df_5min = df.resample('5min').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna().reset_index()

                if df_5min.empty:
                    time.sleep(60)
                    continue

                # Add technical indicators
                df_5min = calculate_all_indicators(df_5min)
                # Manually add SMA200 (our calculate_all_indicators doesn't have it by default)
                df_5min['sma_200'] = df_5min['close'].rolling(window=200).mean()
                df_5min = df_5min.dropna(subset=['sma_200'])

                if len(df_5min) < 1:
                    time.sleep(60)
                    continue

                self.process_bar(df_5min)
                time.sleep(60)  # Check every minute

            except KeyboardInterrupt:
                logger.info("Shutting down gracefully...")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Error: {e}", exc_info=True)
                time.sleep(60)

if __name__ == "__main__":
    trader = LiveTrader(symbol="AAPL")
    trader.run()