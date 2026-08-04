#!/usr/bin/env python3
"""
src/memory/backfill_forward_returns.py  (v2.2, LEGACY 5-min)

v3 NOTE: this script targets the 5-min base tables (market_data /
feature_cache). The 1h layer computes its own forward returns inside
resample_to_1h / backfill_crypto / run_pump, so v3 (1h) rebuilds do NOT
need this script - run update_qdrant_payloads directly after build_memory.

Calculates realised forward returns for every historical bar at BOTH
horizons: 1h (reference) and the active prediction horizon
(config.FORWARD_HORIZON_HOURS, default 4h).

Gap-safe: merge_asof with a 30-minute tolerance - overnight/weekend gaps
never produce fake returns spanning days.

v2.2: writes both columns in ONE batched pass (execute_values) instead of
one UPDATE per row.
"""
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("BackfillForwardReturns", "logs/memory.log")

HORIZON_MIN = config.FORWARD_HORIZON_HOURS * 60


def _horizon_returns(group: pd.DataFrame, horizon_minutes: int, col: str) -> pd.DataFrame:
    group = group.sort_values('time_bucket').reset_index(drop=True)
    group['future_time'] = group['time_bucket'] + pd.Timedelta(minutes=horizon_minutes)
    left = group[['symbol', 'time_bucket', 'future_time']]
    right = group[['time_bucket', 'close']].rename(columns={'time_bucket': 'right_time'})
    merged = pd.merge_asof(left, right, left_on='future_time', right_on='right_time',
                           direction='forward', tolerance=pd.Timedelta(minutes=30))
    merged[col] = (merged['close'] / group['close']) - 1
    return merged[['symbol', 'time_bucket', col]]


def main():
    logger.info(f"===== FORWARD RETURN BACKFILL (1h + {config.FORWARD_HORIZON_HOURS}h, gap-safe) =====")
    conn = psycopg2.connect(config.database.url)

    df = pd.read_sql("""
        SELECT symbol, time_bucket, close
        FROM market_data
        ORDER BY symbol, time_bucket ASC
    """, conn)
    logger.info(f"Loaded {len(df)} price rows.")
    if df.empty:
        return

    h1 = df.groupby('symbol', group_keys=False).apply(_horizon_returns, 60, 'forward_return_1h')
    hN = df.groupby('symbol', group_keys=False).apply(_horizon_returns, HORIZON_MIN, 'forward_return_4h')

    result = h1.merge(hN, on=['symbol', 'time_bucket'], how='outer')
    logger.info(f"Computed horizons for {len(result)} rows (gaps -> NULL).")

    sql = """
        UPDATE feature_cache AS f
        SET forward_return_1h = v.fr1, forward_return_4h = v.fr4
        FROM (VALUES %s) AS v(symbol, time_bucket, fr1, fr4)
        WHERE f.symbol = v.symbol AND f.time_bucket = v.time_bucket::timestamptz
    """
    records = [
        (r.symbol, pd.Timestamp(r.time_bucket).to_pydatetime(),
         None if pd.isna(r.forward_return_1h) else float(r.forward_return_1h),
         None if pd.isna(r.forward_return_4h) else float(r.forward_return_4h))
        for r in result.itertuples(index=False)
    ]

    cur = conn.cursor()
    execute_values(cur, sql, records, page_size=2000)
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"===== BACKFILL COMPLETE: {len(records)} rows (1h + 4h) =====")
    logger.info("Next: python -m src.memory.update_qdrant_payloads")


if __name__ == "__main__":
    main()
