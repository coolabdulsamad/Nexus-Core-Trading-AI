#!/usr/bin/env python3
"""
src/memory/update_qdrant_payloads.py  (v3.3)
Syncs realised forward returns (1h + active horizon) + regime labels from
TimescaleDB into Qdrant.

- Timeframe-aware: reads market_data{BAR_SUFFIX}/feature_cache{BAR_SUFFIX}.
- v3.1: covers EVERY symbol present in the feature table (DB-driven), so
  newly added symbols get payloads without config edits.
- Targets EXACTLY the rows that were encoded into Qdrant (same JOIN,
  same vwap/close not-null rule as the encoder), so point ids match 1:1.
- Sends updates in BATCHES via batch_update_points (one HTTP round trip
  per 100 points instead of one per point).
- v3.3: --days N incremental mode (used by the daily self-maintenance):
  only rows from the last N days are synced. A point's 4h outcome matures
  4h after its bar, so a 10-day window self-heals any sync gap of up to
  10 days while doing ~1% of the writes of a full resync.

Run from project root:  python -m src.memory.update_qdrant_payloads            (full resync)
                        python -m src.memory.update_qdrant_payloads --days 10  (incremental)
Safe to re-run: set_payload just overwrites the same keys.
NOTE: rows whose Qdrant point was never encoded yet (e.g. brand-new pump
rows) simply 404 and are skipped - they get picked up on the next run
after the pump encodes them.
"""
import argparse
import psycopg2
import pandas as pd
from qdrant_client import QdrantClient, models
from config.settings import config
from src.memory.qdrant_memory import make_point_id
from src.utils.logger import setup_logger

logger = setup_logger("UpdateQdrantPayloads", "logs/memory.log")

BATCH_SIZE = 100
HORIZON_KEY = f"forward_return_{config.FORWARD_HORIZON_HOURS}h"


def main():
    parser = argparse.ArgumentParser(description="Sync realised outcomes from TimescaleDB into Qdrant")
    parser.add_argument('--days', type=int, default=None,
                        help='Incremental mode: only sync rows from the last N days. '
                             'Default (no flag): full resync of every memory row.')
    args = parser.parse_args()

    suffix = config.BAR_SUFFIX
    collection = f"market_memory_{config.BAR_MINUTES}m"
    mode = f"incremental (last {args.days} days)" if args.days else "FULL resync"
    logger.info(f"===== QDRANT PAYLOAD UPDATE (collection: {collection}, key: {HORIZON_KEY}, mode: {mode}) =====")

    conn = psycopg2.connect(config.database.url)
    symbols = pd.read_sql(
        f"SELECT DISTINCT symbol FROM feature_cache{suffix} ORDER BY symbol",
        conn)['symbol'].tolist()
    logger.info(f"Syncing payloads for symbols: {symbols}")

    sql = f"""
        SELECT f.symbol, f.time_bucket, f.forward_return_1h, f.forward_return_4h,
               f.regime_label, m.close, m.vwap
        FROM feature_cache{suffix} f
        JOIN market_data{suffix} m
          ON f.symbol = m.symbol AND f.time_bucket = m.time_bucket
        WHERE f.symbol = ANY(%s)
    """
    params = [symbols]
    if args.days:
        sql += " AND f.time_bucket >= NOW() - (%s * INTERVAL '1 day')"
        params.append(args.days)

    df = pd.read_sql(sql, conn, params=params)
    conn.close()

    # Same not-null rule the memory build used, so ids match 1:1.
    df = df.dropna(subset=['vwap', 'close'])
    # Only points that have an outcome at the ACTIVE horizon are useful to the brain.
    df = df[df[HORIZON_KEY].notna()]
    logger.info(f"{len(df)} memory rows with realised {config.FORWARD_HORIZON_HOURS}h outcomes ({mode}).")
    if df.empty:
        logger.warning("Nothing to update - build the data layer first (or widen --days).")
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
                collection_name=collection,
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
        f"(failed batches: {failed_batches}, mode: {mode}) ====="
    )


if __name__ == "__main__":
    main()
