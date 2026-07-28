"""
src/ingestion/loader.py
Responsibility: Writes the enriched data to TimescaleDB (market_data & feature_cache).
Uses batch inserts for maximum performance.
"""
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd
from config.settings import config
from utils.logger import setup_logger

logger = setup_logger("DataLoader", "logs/ingestion.log")

class TimescaleDBLoader:
    def __init__(self):
        self.db_url = config.database.url
        self.conn = None

    def connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(self.db_url)
            self.conn.autocommit = False  # We manage transactions manually
        return self.conn

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.commit()
            self.conn.close()

    def insert_market_data(self, df: pd.DataFrame, symbol: str, exchange: str = "NYSE"):
        """
        Inserts raw OHLCV + VWAP into the 'market_data' hypertable.
        """
        if df.empty:
            logger.warning("No data to insert into market_data.")
            return

        conn = self.connect()
        cur = conn.cursor()
        
        # Prepare data for bulk insert: list of tuples
        records = []
        for _, row in df.iterrows():
            records.append((
                symbol,
                exchange,
                row['timestamp'],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume'],
                row['vwap']
            ))

        # Bulk insert SQL
        insert_query = """
            INSERT INTO market_data 
            (symbol, exchange, time_bucket, open, high, low, close, volume, vwap)
            VALUES %s
            ON CONFLICT (symbol, exchange, time_bucket) 
            DO UPDATE SET 
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                vwap = EXCLUDED.vwap;
        """
        
        try:
            execute_values(cur, insert_query, records, page_size=1000)
            conn.commit()
            logger.info(f"Inserted/Updated {len(records)} rows into market_data for {symbol}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to insert market_data: {str(e)}")
            raise
        finally:
            cur.close()

    def insert_feature_cache(self, df: pd.DataFrame, symbol: str):
        """
        Inserts the calculated indicators + sentiment_score into 'feature_cache'.
        """
        if df.empty:
            return

        conn = self.connect()
        cur = conn.cursor()

        records = []
        for _, row in df.iterrows():
            # Ensure sentiment_score exists, else default to 0
            sentiment = row.get('sentiment_score', 0.0)
            records.append((
                symbol,
                row['timestamp'],
                row.get('rsi_14'),
                row.get('macd_line'),
                row.get('macd_signal'),
                row.get('bb_upper'),
                row.get('bb_lower'),
                row.get('atr_14'),
                row.get('volume_profile_ratio'),
                None,  # correlation_spy (Phase 3)
                sentiment
            ))

        # Updated SQL with sentiment_score
        insert_query = """
            INSERT INTO feature_cache 
            (symbol, time_bucket, rsi_14, macd_line, macd_signal, bb_upper, bb_lower, atr_14, volume_profile_ratio, correlation_spy, sentiment_score)
            VALUES %s
            ON CONFLICT (symbol, time_bucket) 
            DO UPDATE SET 
                rsi_14 = EXCLUDED.rsi_14,
                macd_line = EXCLUDED.macd_line,
                macd_signal = EXCLUDED.macd_signal,
                bb_upper = EXCLUDED.bb_upper,
                bb_lower = EXCLUDED.bb_lower,
                atr_14 = EXCLUDED.atr_14,
                volume_profile_ratio = EXCLUDED.volume_profile_ratio,
                sentiment_score = EXCLUDED.sentiment_score;
        """

        try:
            execute_values(cur, insert_query, records, page_size=1000)
            conn.commit()
            logger.info(f"Inserted/Updated {len(records)} rows into feature_cache for {symbol}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to insert feature_cache: {str(e)}")
            raise
        finally:
            cur.close()