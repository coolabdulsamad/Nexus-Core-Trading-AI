#!/usr/bin/env python3
"""
src/ingestion/recompute_features.py
ONE-TIME historical feature recompute (run once, then forget).

Why this exists
---------------
Migration 002 added the v2 indicator columns (adx_14, dist_sma50/200/vwap,
macd_hist, bb_pct_b, bb_width, atr_pct, vol_z, ret_1/3/12, hour_sin/cos,
regime_label) but they were only computed for bars ingested AFTER that
(the last ~754 bars per symbol). Every older feature_cache row has NULL
v2 features, so:

- memory vectors were encoded with those features zero-filled -> every
  state looked nearly identical (sim 0.99 everywhere) -> the brain's
  recall was essentially noise,
- regime_label stayed 'unknown' for 97.6% of history -> the regime
  filter never engaged.

This script recomputes indicators v2 over the FULL market_data history
per symbol (correct SMA200/ADX warm-up, daily-reset VWAP) and upserts
ONLY the indicator columns. forward_return_1h and sentiment_score are
left untouched.

Afterwards:
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

logger = setup_logger("RecomputeFeatures", "logs/ingestion.log")

# Columns we never overwrite (outcomes / live-only data / keys).
PRESERVE = {"symbol", "time_bucket", "forward_return_1h", "sentiment_score"}


def _feature_cache_columns(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'feature_cache'
    """)
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    return cols


def _convert(col, val):
    if val is None or (isinstance(val, float) and pd.isna(val)) or pd.isna(val):
        return None
    if col == 'regime_label':
        return str(val)
    return float(val)


def main():
    logger.info("===== HISTORICAL FEATURE RECOMPUTE =====")
    conn = psycopg2.connect(config.database.url)
    existing = set(_feature_cache_columns(conn))

    for symbol in config.symbols:
        df_raw = pd.read_sql(
            """SELECT time_bucket AS timestamp, open, high, low, close, volume
               FROM market_data WHERE symbol = %s ORDER BY time_bucket""",
            conn, params=(symbol,))
        if df_raw.empty:
            logger.warning(f"{symbol}: no market_data rows, skipped")
            continue

        df = calculate_all_indicators(df_raw)
        if df.empty:
            logger.warning(f"{symbol}: indicator frame empty, skipped")
            continue

        update_cols = [c for c in df.columns if c in existing and c not in PRESERVE]
        all_cols = ["symbol", "time_bucket"] + update_cols
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        sql = (
            f"INSERT INTO feature_cache ({', '.join(all_cols)}) VALUES %s "
            f"ON CONFLICT (symbol, time_bucket) DO UPDATE SET {set_clause}"
        )

        records = [
            (symbol, pd.Timestamp(row['timestamp']).to_pydatetime(),
             *[_convert(c, row[c]) for c in update_cols])
            for _, row in df.iterrows()
        ]

        cur = conn.cursor()
        execute_values(cur, sql, records, page_size=2000)
        conn.commit()
        cur.close()
        logger.info(f"{symbol}: recomputed + upserted {len(records)} feature rows "
                    f"({len(update_cols)} indicator columns)")

    conn.close()
    logger.info("===== RECOMPUTE COMPLETE -> next: build_memory, "
                "update_qdrant_payloads, backtest =====")


if __name__ == "__main__":
    main()
