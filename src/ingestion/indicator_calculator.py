"""
src/ingestion/indicator_calculator.py
Feature Engineering v2 - a much richer state description for the brain.

Adds on top of the classic set:
- MACD histogram, Bollinger %B and band width
- ATR as % of price (volatility, scale-free)
- Multi-horizon returns (1 / 3 / 12 bars = momentum)
- Volume z-score
- ADX (trend strength), SMA50/200 distance in ATR units
- Time-of-day encoding (sin/cos)
- regime_label: trend_up / trend_down / range / transition
"""
import numpy as np
import pandas as pd
import talib as ta
from src.utils.logger import setup_logger

logger = setup_logger("IndicatorCalculator", "logs/ingestion.log")


def _regime(row) -> str:
    adx = row['adx_14']
    if pd.isna(adx) or pd.isna(row['dist_sma50']):
        return 'unknown'
    if adx >= 25:
        return 'trend_up' if row['dist_sma50'] > 0 else 'trend_down'
    if adx < 20:
        return 'range'
    return 'transition'


def calculate_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logger.warning("Empty DataFrame passed to indicator calculator.")
        return df

    df = df.sort_values('timestamp').reset_index(drop=True)
    close, high, low, vol = df['close'], df['high'], df['low'], df['volume']

    # --- Classic set ---
    df['rsi_14'] = ta.RSI(close, timeperiod=14)
    df['macd_line'], df['macd_signal'], df['macd_hist'] = ta.MACD(
        close, fastperiod=12, slowperiod=26, signalperiod=9)
    df['bb_upper'], df['bb_middle'], df['bb_lower'] = ta.BBANDS(
        close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
    df['atr_14'] = ta.ATR(high, low, close, timeperiod=14)

    vol_ma20 = vol.rolling(20).mean()
    df['volume_profile_ratio'] = vol / vol_ma20

    # --- New features ---
    # Bollinger %B and width (scale-free)
    band = (df['bb_upper'] - df['bb_lower']).replace(0, np.nan)
    df['bb_pct_b'] = (close - df['bb_lower']) / band
    df['bb_width'] = band / close

    # Volatility as % of price
    df['atr_pct'] = df['atr_14'] / close

    # Momentum: multi-horizon bar returns
    df['ret_1'] = close.pct_change(1)
    df['ret_3'] = close.pct_change(3)
    df['ret_12'] = close.pct_change(12)

    # Volume z-score (unusual activity)
    vol_std20 = vol.rolling(20).std().replace(0, np.nan)
    df['vol_z'] = (vol - vol_ma20) / vol_std20

    # Trend strength + normalized distances
    df['adx_14'] = ta.ADX(high, low, close, timeperiod=14)
    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()
    atr_safe = df['atr_14'].replace(0, np.nan)
    df['dist_sma50'] = (close - sma50) / atr_safe
    df['dist_sma200'] = (close - sma200) / atr_safe

    # Time of day (market rhythm: open/close behave differently)
    ts = pd.to_datetime(df['timestamp'])
    minutes = ts.dt.hour * 60 + ts.dt.minute
    df['hour_sin'] = np.sin(2 * np.pi * minutes / 1440)
    df['hour_cos'] = np.cos(2 * np.pi * minutes / 1440)

    # --- VWAP (resets daily) ---
    df['date'] = ts.dt.date
    cum_vp = (vol * close).groupby(df['date']).cumsum()
    cum_v = vol.groupby(df['date']).cumsum()
    df['vwap'] = cum_vp / cum_v.replace(0, np.nan)
    df['dist_vwap'] = (close - df['vwap']) / atr_safe

    # --- Regime label ---
    df['regime_label'] = df.apply(_regime, axis=1)

    df = df.drop(columns=['date'])
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna()

    logger.info(f"Indicators v2 calculated. {len(df)} rows after warm-up drop.")
    return df
