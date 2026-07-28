#!/usr/bin/env python3
"""
src/memory/backfill_forward_returns.py
Calculates the realised 1-hour forward return for every historical bar.

FIXED: merge_asof now has a 30-minute tolerance - overnight/weekend gaps no
longer produce fake "1h returns" that actually span days.
"""
import psycopg2
import pandas as pd
from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("BackfillForwardReturns", "logs/memory.log")


def main():
    logger.info("===== FORWARD RETURN BACKFILL (gap-safe) =====")
    conn = psycopg2.connect(config.database.url)

    df = pd.read_sql("""
        SELECT symbol, time_bucket, close
        FROM market_data
        ORDER BY symbol, time_bucket ASC
    """, conn)
    logger.info(f"Loaded {len(df)} price rows.")
    if df.empty:
        return

    def compute(group):
        group = group.sort_values('time_bucket').reset_index(drop=True)
        group['future_time'] = group['time_bucket'] + pd.Timedelta(minutes=60)
        left = group[['symbol', 'time_bucket', 'future_time']]
        right = group[['time_bucket', 'close']].rename(columns={'time_bucket': 'right_time'})
        merged = pd.merge_asof(left, right, left_on='future_time', right_on='right_time',
                               direction='forward', tolerance=pd.Timedelta(minutes=30))
        merged['forward_return_1h'] = (merged['close'] / group['close']) - 1
        return merged[['symbol', 'time_bucket', 'forward_return_1h']]

    result = df.groupby('symbol', group_keys=False).apply(compute)
    result = result.dropna(subset=['forward_return_1h'])
    logger.info(f"Forward returns computed for {len(result)} rows (gaps excluded).")

    cur = conn.cursor()
    updated = 0
    for _, row in result.iterrows():
        cur.execute("""
            UPDATE feature_cache SET forward_return_1h = %s
            WHERE symbol = %s AND time_bucket = %s
        """, (row['forward_return_1h'], row['symbol'], row['time_bucket']))
        updated += 1
        if updated % 500 == 0:
            conn.commit()
            logger.info(f"Committed {updated} rows...")
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"===== BACKFILL COMPLETE: {updated} rows =====")


if __name__ == "__main__":
    main()
