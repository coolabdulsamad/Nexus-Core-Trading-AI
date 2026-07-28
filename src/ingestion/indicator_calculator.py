"""
src/ingestion/indicator_calculator.py
Responsibility: Adds technical indicators to the raw DataFrame.
This is the "Feature Engineering" stage.
"""
import pandas as pd
import numpy as np
import talib as ta
from utils.logger import setup_logger

logger = setup_logger("IndicatorCalculator", "logs/ingestion.log")

def calculate_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a DataFrame with 'open', 'high', 'low', 'close', 'volume'.
    Returns the same DataFrame with additional indicator columns.
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to indicator calculator.")
        return df

    logger.info("Calculating technical indicators...")
    
    # Ensure we are sorted by time
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # 1. Relative Strength Index (RSI)
    df['rsi_14'] = ta.RSI(df['close'], timeperiod=14)
    
    # 2. MACD
    df['macd_line'], df['macd_signal'], _ = ta.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    
    # 3. Bollinger Bands (20, 2)
    df['bb_upper'], df['bb_middle'], df['bb_lower'] = ta.BBANDS(df['close'], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
    
    # 4. Average True Range (ATR) - Critical for Risk Management
    df['atr_14'] = ta.ATR(df['high'], df['low'], df['close'], timeperiod=14)
    
    # 5. Volume Profile (Ratio of current volume to 20-day average volume)
    df['volume_ma_20'] = df['volume'].rolling(window=20).mean()
    df['volume_profile_ratio'] = df['volume'] / df['volume_ma_20']
    
    # 6. VWAP (Volume Weighted Average Price) - Calculated cumulatively per day.
    # We group by date to reset VWAP each trading day.
    df['date'] = df['timestamp'].dt.date
    df['cum_vol_price'] = (df['volume'] * df['close']).groupby(df['date']).cumsum()
    df['cum_vol'] = df['volume'].groupby(df['date']).cumsum()
    df['vwap'] = df['cum_vol_price'] / df['cum_vol']
    
    # 7. Drop helper columns and rows with NaN (since indicators need warm-up periods)
    df = df.drop(['date', 'cum_vol_price', 'cum_vol', 'volume_ma_20'], axis=1)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna()
    
    logger.info(f"Indicators calculated. {len(df)} rows remaining after dropping NaN.")
    return df