#!/usr/bin/env python3
"""
src/memory/update_qdrant_payloads.py
Syncs realised forward returns (1h + active horizon) + regime labels from
TimescaleDB into Qdrant.

- Targets EXACTLY the rows that were encoded into Qdrant (same JOIN,
  same symbol filter, same vwap/close not-null rule as
  vector_encoder.fetch_all_features), so point ids match 1:1.
- Sends updates in BATCHES via batch_update_points (one HTTP round trip
  per 100 points instead of one per point).

Run from project root:  python -m src.memory.update_qdrant_payloads
Safe to re-run: set_payload just overwrites the same keys.
"""
import psycopg2
import pandas as pd
from qdrant_client import QdrantClient, models
from config.settings import config
from src.memory.qdrant_memory import make_point_id
from src.utils.logger import setup_logger

logger = setup_logger("UpdateQdrantPayloads", "logs/memory.log")

COLLECTION = "market_memory"
BATCH_SIZE = 100
HORIZON_KEY = f"forward_return_{config.FORWARD_HORIZON_HOURS}h"


def main():
    logger.info(f"===== QDRANT PAYLOAD UPDATE (batched, horizon key: {HORIZON_KEY}) =====")
    symbols = list(config.symbols)

    conn = psycopg2.connect(config.database.url)
    df = pd.read_sql(
        """
        SELECT f.symbol, f.time_bucket, f.forward_return_1h, f.forward_return_4h,
               f.regime_label, m.close, m.vwap
        FROM feature_cache f
        JOIN market_data m
          ON f.symbol = m.symbol AND f.time_bucket = m.time_bucket
        WHERE f.symbol = ANY(%s)
        """,
        conn,
        params=(symbols,),
    )
    conn.close()

    # Same not-null rule the memory build used, so ids match 1:1.
    df = df.dropna(subset=['vwap', 'close'])
    # Only points that have an outcome at the ACTIVE horizon are useful to the brain.
    df = df[df[HORIZON_KEY].notna()]
    logger.info(f"{len(df)} memory rows with realised {config.FORWARD_HORIZON_HOURS}h outcomes.")
    if df.empty:
        logger.warning("Nothing to update - run backfill_forward_returns first.")
        return

    df['point_id'] = [
        make_point_id(sym, int(pd.Timestamp(ts).timestamp()))
        for sym, ts in zip(df['symbol'], df['time_bucket'])
    ]

    client = QdrantClient(host=config.qdrant.host, port=config.qdrant.port)
    updated = 0
    failed_batches = 0

    def flush(ops):
        nonlocal updated, failed_batches
        try:
            client.batch_update_points(
                collection_name=COLLECTION,
                update_operations=ops,
                wait=True,
            )
            updated += len(ops)
            if updated % 10000 < BATCH_SIZE:
                logger.info(f"Updated {updated}/{len(df)} payloads...")
        except Exception as e:
            failed_batches += 1
            logger.error(f"Batch of {len(ops)} ops failed: {e}")

    ops = []
    for row in df.itertuples(index=False):
        payload = {
            HORIZON_KEY: float(getattr(row, HORIZON_KEY)),
            "regime_label": row.regime_label or 'unknown',
        }
        fr1 = getattr(row, 'forward_return_1h', None)
        if fr1 is not None and not pd.isna(fr1):
            payload["forward_return_1h"] = float(fr1)   # kept as reference
        ops.append(models.SetPayloadOperation(
            set_payload=models.SetPayload(payload=payload, points=[row.point_id])
        ))
        if len(ops) >= BATCH_SIZE:
            flush(ops)
            ops = []
    if ops:
        flush(ops)

    logger.info(
        f"===== PAYLOAD UPDATE COMPLETE: {updated}/{len(df)} points "
        f"(failed batches: {failed_batches}) ====="
    )


if __name__ == "__main__":
    main()
