#!/usr/bin/env python3
"""
src/ingestion/resample_to_1h.py
Builds the complete 1-hour data layer from the existing 5-min market_data:

  1. resamples OHLCV to 1h bars (bins anchored at :30 so session bars are
     clean 9:30-10:30, 10:30-11:30, ... for US equities)
  2. computes the full v2 indicator set on the 1h series (proper warm-up,
     daily-reset VWAP, regime labels)
  3. computes forward returns on the 1h series (1 bar = 1h, 4 bars = 4h)
  4. upserts market_data_1h + feature_cache_1h (5-min tables untouched)

Run ONCE after migration 005:
    python -m src.ingestion.resample_to_1h
Then:
    python -m src.memory.build_memory
    python -m src.memory.update_qdrant_payloads
    python -m src.backtester.engine AAPL 2026-03-24 2026-06-24
"""
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from config.settings import config
from src.ingestion.indicator_calculator import calculate_all_indicators
from src.utils.logger import setup_logger

logger = setup_logger("ResampleTo1h", "logs/ingestion.log")

MD_COLS = ["symbol", "time_bucket", "open", "high", "low", "close", "volume", "vwap"]
PRESERVE = {"symbol", "time_bucket", "sentiment_score"}


def _table_columns(conn, table):
    cur = conn.cursor()
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
        (table,))
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    return cols


def _convert(col, val):
    if val is None or pd.isna(val):
        return None
    if col == 'regime_label':
        return str(val)
    return float(val)


def _resample_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['vwap_num'] = df['vwap'] * df['volume']
    df = df.set_index('timestamp').sort_index()
    out = df.resample('1h', origin='start_day', offset='30min').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
        'volume': 'sum', 'vwap_num': 'sum',
    }).dropna(subset=['close'])
    out['vwap'] = out['vwap_num'] / out['volume'].replace(0, pd.NA)
    return out.drop(columns=['vwap_num']).reset_index()


def main():
    logger.info("===== 1H DATA LAYER BUILD =====")
    conn = psycopg2.connect(config.database.url)
    fc_cols = set(_table_columns(conn, 'feature_cache_1h'))
    if not fc_cols:
        logger.error("feature_cache_1h missing - run migration 005 first")
        return

    for symbol in config.symbols:
        df5 = pd.read_sql(
            """SELECT time_bucket AS timestamp, open, high, low, close, volume, vwap
               FROM market_data WHERE symbol = %s ORDER BY time_bucket""",
            conn, params=(symbol,))
        if df5.empty:
            logger.warning(f"{symbol}: no 5-min data, skipped")
            continue

        df1 = _resample_ohlcv(df5)

        # Forward returns on the FULL 1h series (bar-shift; overnight moves count)
        df1['forward_return_1h'] = df1['close'].shift(-1) / df1['close'] - 1
        df1['forward_return_4h'] = df1['close'].shift(-4) / df1['close'] - 1

        # --- upsert market_data_1h ---
        md_records = [
            (symbol, pd.Timestamp(r['timestamp']).to_pydatetime(),
             float(r['open']), float(r['high']), float(r['low']), float(r['close']),
             float(r['volume']), None if pd.isna(r['vwap']) else float(r['vwap']))
            for _, r in df1.iterrows()
        ]
        md_sql = (
            f"INSERT INTO market_data_1h ({', '.join(MD_COLS)}) VALUES %s "
            f"ON CONFLICT (symbol, time_bucket) DO UPDATE SET "
            + ", ".join(f"{c} = EXCLUDED.{c}" for c in MD_COLS if c not in ('symbol', 'time_bucket'))
        )
        cur = conn.cursor()
        execute_values(cur, md_sql, md_records, page_size=1000)
        conn.commit()

        # --- indicators on the 1h frame ---
        feat = calculate_all_indicators(
            df1[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy())
        if feat.empty:
            logger.warning(f"{symbol}: indicator frame empty, skipped")
            continue
        feat = feat.merge(
            df1[['timestamp', 'forward_return_1h', 'forward_return_4h']],
            on='timestamp', how='left')

        update_cols = [c for c in feat.columns
                       if c in fc_cols and c not in PRESERVE and c != 'timestamp']
        all_cols = ["symbol", "time_bucket"] + update_cols
        fc_sql = (
            f"INSERT INTO feature_cache_1h ({', '.join(all_cols)}) VALUES %s "
            f"ON CONFLICT (symbol, time_bucket) DO UPDATE SET "
            + ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        )
        fc_records = [
            (symbol, pd.Timestamp(r['timestamp']).to_pydatetime(),
             *[_convert(c, r[c]) for c in update_cols])
            for _, r in feat.iterrows()
        ]
        execute_values(cur, fc_sql, fc_records, page_size=1000)
        conn.commit()
        cur.close()
        logger.info(f"{symbol}: {len(md_records)} 1h bars | {len(fc_records)} feature rows")

    conn.close()
    logger.info("===== 1H BUILD COMPLETE -> build_memory, update_qdrant_payloads, backtest =====")


if __name__ == "__main__":
    main()
