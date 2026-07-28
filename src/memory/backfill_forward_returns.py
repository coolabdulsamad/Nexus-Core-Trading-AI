#!/usr/bin/env python3
"""
src/memory/backfill_forward_returns.py
Responsibility: Calculate the 1-hour forward return for every historical row.
Uses timestamp-based lookup (merge_asof) for accuracy.
"""
import psycopg2
import pandas as pd
import numpy as np
from config.settings import config
from utils.logger import setup_logger

logger = setup_logger("BackfillForwardReturns", "logs/memory.log")

def main():
    logger.info("===== STARTING FORWARD RETURN BACKFILL (timestamp-based) =====")
    
    conn = psycopg2.connect(config.database.url)
    
    # 1. Fetch all data with timestamps and prices
    query = """
        SELECT 
            symbol, 
            time_bucket, 
            close
        FROM market_data
        ORDER BY symbol, time_bucket ASC
    """
    df = pd.read_sql(query, conn)
    logger.info(f"Loaded {len(df)} price rows.")
    
    if df.empty:
        logger.error("No data found.")
        return
    
    # 2. For each symbol, compute forward return using merge_asof
    def compute_forward_returns(group):
        group = group.sort_values('time_bucket').reset_index(drop=True)
        # Create a future timestamp: 60 minutes later
        group['future_time'] = group['time_bucket'] + pd.Timedelta(minutes=60)
        # Merge with itself using asof to get closest future price
        # We need to keep only the columns we need; we'll use merge_asof
        # Prepare two dataframes: left has current time and future_time; right has time_bucket and close
        left = group[['symbol', 'time_bucket', 'future_time']]
        right = group[['time_bucket', 'close']].rename(columns={'time_bucket': 'right_time'})
        merged = pd.merge_asof(left, right, left_on='future_time', right_on='right_time', direction='forward')
        # merged now has 'close' as the future close
        merged['forward_return_1h'] = (merged['close'] / group['close']) - 1
        return merged[['symbol', 'time_bucket', 'forward_return_1h']]
    
    result = df.groupby('symbol', group_keys=False).apply(compute_forward_returns)
    result = result.dropna(subset=['forward_return_1h'])
    logger.info(f"Calculated forward returns for {len(result)} rows.")
    
    # 3. Update the feature_cache table
    cur = conn.cursor()
    updated_count = 0
    for _, row in result.iterrows():
        update_query = """
            UPDATE feature_cache 
            SET forward_return_1h = %s
            WHERE symbol = %s AND time_bucket = %s
        """
        cur.execute(update_query, (row['forward_return_1h'], row['symbol'], row['time_bucket']))
        updated_count += 1
        if updated_count % 500 == 0:
            conn.commit()
            logger.info(f"Committed {updated_count} rows...")
    
    conn.commit()
    cur.close()
    conn.close()
    
    logger.info(f"===== BACKFILL COMPLETED. Updated {updated_count} rows. =====")

if __name__ == "__main__":
    main()