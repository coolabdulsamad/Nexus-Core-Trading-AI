#!/usr/bin/env python3
"""
src/memory/update_qdrant_payloads.py
Syncs realised forward returns + regime labels from TimescaleDB into Qdrant.
FIXED: uses the same uuid5 point IDs as qdrant_memory.make_point_id.
"""
import psycopg2
import pandas as pd
from qdrant_client import QdrantClient
from config.settings import config
from src.memory.qdrant_memory import make_point_id
from src.utils.logger import setup_logger

logger = setup_logger("UpdateQdrantPayloads", "logs/memory.log")


def main():
    logger.info("===== QDRANT PAYLOAD UPDATE =====")
    conn = psycopg2.connect(config.database.url)
    df = pd.read_sql("""
        SELECT symbol, time_bucket, forward_return_1h, regime_label
        FROM feature_cache
        WHERE forward_return_1h IS NOT NULL
    """, conn)
    conn.close()
    logger.info(f"{len(df)} rows with outcomes.")
    if df.empty:
        logger.warning("Nothing to update - run backfill_forward_returns first.")
        return

    client = QdrantClient(host=config.qdrant.host, port=config.qdrant.port)
    updated = 0
    for _, row in df.iterrows():
        ts_epoch = int(pd.Timestamp(row['time_bucket']).timestamp())
        point_id = make_point_id(row['symbol'], ts_epoch)
        try:
            client.set_payload(
                collection_name="market_memory",
                payload={
                    "forward_return_1h": float(row['forward_return_1h']),
                    "regime_label": row.get('regime_label') or 'unknown',
                },
                points=[point_id],
            )
            updated += 1
            if updated % 500 == 0:
                logger.info(f"Updated {updated} payloads...")
        except Exception as e:
            logger.error(f"Failed on point {point_id}: {e}")

    logger.info(f"===== PAYLOAD UPDATE COMPLETE: {updated} points =====")


if __name__ == "__main__":
    main()
