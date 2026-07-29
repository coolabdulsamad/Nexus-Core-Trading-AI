#!/usr/bin/env python3
"""
src/ingestion/backfill_history.py
Backfills ~2 years of 5-min bars from Polygon into market_data.

Why: the 1h brain is directionally right (returns -8.5% avg -> breakeven,
signal_flip win rate 25% -> 50-100%) but the 1h memory is only ~5.7k
states (~160-300 days/symbol), giving 4-8 trades per symbol - too few
for a statistically meaningful verdict. Two years of history ~= 16k 1h
states.

Polygon free tier ~= 5 requests/min -> 6-month chunks with pauses.

    python -m src.ingestion.backfill_history [years]     # default 2
Then:
    python -m src.ingestion.resample_to_1h
    python -m src.memory.build_memory
    python -m src.memory.update_qdrant_payloads
    for s in AAPL TSLA NVDA MSFT GOOGL; do python -m src.backtester.engine $s 2026-03-24 2026-06-24 2>/dev/null | grep -A 12 "HONEST AI BACKTEST"; done
"""
import sys
import time
import pandas as pd
from config.settings import config
from src.ingestion.fetcher import PolygonFetcher
from src.ingestion.loader import TimescaleDBLoader
from src.utils.logger import setup_logger

logger = setup_logger("BackfillHistory", "logs/ingestion.log")

YEARS = float(sys.argv[1]) if len(sys.argv) > 1 else 2.0
CHUNK_DAYS = 180
PAUSE_S = 13                      # free-tier friendly (~4.6 calls/min)


def main():
    fetcher = PolygonFetcher()
    loader = TimescaleDBLoader()
    end = pd.Timestamp.utcnow().normalize()
    start_all = end - pd.Timedelta(days=int(YEARS * 365))

    for symbol in config.symbols:
        logger.info(f"===== {symbol}: backfilling {start_all.date()} -> {end.date()} =====")
        chunk_start = start_all
        total = 0
        while chunk_start < end:
            chunk_end = min(chunk_start + pd.Timedelta(days=CHUNK_DAYS), end)
            df = fetcher.fetch_historical_bars(
                symbol, str(chunk_start.date()), str(chunk_end.date()), "minute", 5)
            if not df.empty:
                loader.insert_market_data(df, symbol)
                total += len(df)
            time.sleep(PAUSE_S)
            chunk_start = chunk_end
        logger.info(f"{symbol}: {total} bars backfilled")

    loader.close()
    logger.info("===== BACKFILL COMPLETE -> resample_to_1h, build_memory, "
                "update_qdrant_payloads, backtest =====")


if __name__ == "__main__":
    main()
