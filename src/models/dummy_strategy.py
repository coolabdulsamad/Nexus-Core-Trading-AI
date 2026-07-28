"""
src/models/dummy_strategy.py
Responsibility: Generates "BUY", "SELL", or "HOLD" signals based on RSI + VWAP.
This is a MEAN-REVERSION strategy for testing the engine.
"""
import pandas as pd

class DummyStrategy:
    def __init__(self):
        self.name = "RSI_MeanReversion"

    def generate_signal(self, row: pd.Series) -> str:
        """
        Returns: 'BUY', 'SELL', or 'HOLD'
        Logic: 
        - Buy when RSI drops below 30 (oversold) AND price is above VWAP (uptrend pullback).
        - Sell (close position) when RSI crosses above 70 (overbought) OR price falls below VWAP.
        """
        # If we don't have indicators, just hold
        if pd.isna(row.get('rsi_14')) or pd.isna(row.get('vwap')):
            return 'HOLD'

        rsi = row['rsi_14']
        price = row['close']
        vwap = row['vwap']

        # Condition to BUY: Oversold + Price holding above VWAP (bullish structure)
        if rsi < 30 and price > vwap:
            return 'BUY'
        
        # Condition to SELL/EXIT: Overbought OR Price broke below VWAP
        if rsi > 70 or price < vwap:
            return 'SELL'
        
        return 'HOLD'