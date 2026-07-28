#!/usr/bin/env python3
"""
src/memory/backfill_regime_labels.py
Recomputes regime_label for every feature_cache row from adx_14 +
dist_sma50 (same rule as indicator_calculator._regime) so the brain's
regime-filtered recall actually engages on historical memory.

Why: migration 002 added regime_label with default 'unknown', and only
rows ingested AFTER that got real labels. The entire historical memory
was 'unknown' -> MetaLearner silently skipped the regime filter.

Run once:   python -m src.memory.backfill_regime_labels
Then sync:  python -m src.memory.update_qdrant_payloads
"""
import psycopg2
from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("BackfillRegimeLabels", "logs/memory.log")

SQL_UPDATE = """
UPDATE feature_cache
SET regime_label = CASE
    WHEN adx_14 IS NULL OR dist_sma50 IS NULL THEN 'unknown'
    WHEN adx_14 >= 25 AND dist_sma50 > 0 THEN 'trend_up'
    WHEN adx_14 >= 25 THEN 'trend_down'
    WHEN adx_14 < 20 THEN 'range'
    ELSE 'transition'
END
"""

SQL_DIST = """
SELECT regime_label, COUNT(*) AS n
FROM feature_cache
GROUP BY regime_label
ORDER BY n DESC
"""


def main():
    logger.info("===== REGIME LABEL BACKFILL =====")
    conn = psycopg2.connect(config.database.url)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(SQL_UPDATE)
    logger.info(f"Recomputed regime_label on {cur.rowcount} rows.")

    cur.execute(SQL_DIST)
    for label, n in cur.fetchall():
        logger.info(f"  {label:<12} {n}")

    cur.close()
    conn.close()
    logger.info("Done. Now run: python -m src.memory.update_qdrant_payloads")


if __name__ == "__main__":
    main()
