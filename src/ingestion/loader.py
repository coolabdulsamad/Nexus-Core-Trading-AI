"""
src/ingestion/loader.py
Writes enriched data to TimescaleDB (market_data & feature_cache).
v2: feature_cache now stores the full Brain-v2 feature set + regime label.
"""
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("DataLoader", "logs/ingestion.log")

FEATURE_COLS_DB = [
    'rsi_14', 'macd_line', 'macd_signal', 'macd_hist',
    'bb_upper', 'bb_lower', 'bb_pct_b', 'bb_width',
    'atr_14', 'atr_pct', 'volume_profile_ratio', 'vol_z',
    'ret_1', 'ret_3', 'ret_12',
    'adx_14', 'dist_sma50', 'dist_sma200', 'dist_vwap',
    'hour_sin', 'hour_cos',
    'regime_label', 'sentiment_score',
]


class TimescaleDBLoader:
    def __init__(self):
        self.db_url = config.database.url
        self.conn = None

    def connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(self.db_url)
            self.conn.autocommit = False
        return self.conn

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.commit()
            self.conn.close()

    def insert_market_data(self, df: pd.DataFrame, symbol: str, exchange: str = "NYSE"):
        if df.empty:
            return
        conn = self.connect()
        cur = conn.cursor()
        records = [(
            symbol, exchange, row['timestamp'], row['open'], row['high'],
            row['low'], row['close'], row['volume'], row.get('vwap'),
        ) for _, row in df.iterrows()]

        try:
            execute_values(cur, """
                INSERT INTO market_data
                (symbol, exchange, time_bucket, open, high, low, close, volume, vwap)
                VALUES %s
                ON CONFLICT (symbol, exchange, time_bucket) DO UPDATE SET
                    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                    close = EXCLUDED.close, volume = EXCLUDED.volume, vwap = EXCLUDED.vwap;
            """, records, page_size=1000)
            conn.commit()
            logger.info(f"market_data: {len(records)} rows upserted for {symbol}")
        except Exception as e:
            conn.rollback()
            logger.error(f"market_data insert failed: {e}")
            raise
        finally:
            cur.close()

    def insert_feature_cache(self, df: pd.DataFrame, symbol: str):
        if df.empty:
            return
        conn = self.connect()
        cur = conn.cursor()

        records = []
        for _, row in df.iterrows():
            records.append(tuple([symbol, row['timestamp']] + [
                row.get(col) if col != 'regime_label' else (row.get(col) or 'unknown')
                for col in FEATURE_COLS_DB
            ]))

        col_list = ", ".join(FEATURE_COLS_DB)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in FEATURE_COLS_DB if c != 'regime_label')

        try:
            execute_values(cur, f"""
                INSERT INTO feature_cache (symbol, time_bucket, {col_list})
                VALUES %s
                ON CONFLICT (symbol, time_bucket) DO UPDATE SET {updates};
            """, records, page_size=1000)
            conn.commit()
            logger.info(f"feature_cache: {len(records)} rows upserted for {symbol}")
        except Exception as e:
            conn.rollback()
            logger.error(f"feature_cache insert failed: {e}")
            raise
        finally:
            cur.close()
