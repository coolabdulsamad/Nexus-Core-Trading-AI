#!/usr/bin/env python3
"""
src/ingestion/backfill_history.py
Backfills ~2 years of 5-min bars from Polygon into market_data.

Why: the 1h brain is directionally right but needs deep history to recall
from. Two years of 5-min bars ~= 16k 1h states per symbol after resampling.

v3.1: the symbol list is DB-driven - every ACTIVE stock in the universe
(seed it first with: python -m src.universe.seed_universe). Crypto is
skipped here (Polygon free has no crypto; use backfill_crypto instead).
You can still pass explicit symbols on the CLI.

Polygon free tier ~= 5 requests/min -> 6-month chunks with pauses.

    python -m src.ingestion.backfill_history [years] [SYMBOL ...]
Then:
    python -m src.ingestion.backfill_crypto 2
    python -m src.ingestion.resample_to_1h
    python -m src.memory.build_memory
    python -m src.memory.update_qdrant_payloads
"""
import sys
import time
import pandas as pd
from config.settings import config
from src.ingestion.fetcher import PolygonFetcher
from src.ingestion.loader import TimescaleDBLoader
from src.utils.logger import setup_logger

logger = setup_logger("BackfillHistory", "logs/ingestion.log")

CHUNK_DAYS = 180
PAUSE_S = 13                      # free-tier friendly (~4.6 calls/min)


def _resolve_symbols(cli_symbols):
    if cli_symbols:
        return [s.upper() for s in cli_symbols]
    try:
        from src.universe.symbol_manager import SymbolManager
        rows = SymbolManager().list_symbols(active_only=True)
        stocks = [r['symbol'] for r in rows if r.get('asset_type') == 'stock']
        if stocks:
            logger.info(f"Universe from DB: {len(stocks)} active stocks")
            return stocks
    except Exception as e:
        logger.warning(f"DB universe unavailable ({e}) -> config fallback")
    return list(config.symbols)


def main():
    args = sys.argv[1:]
    years = 2.0
    if args:
        try:
            years = float(args[0])
            args = args[1:]
        except ValueError:
            pass
    symbols = _resolve_symbols(args)

    fetcher = PolygonFetcher()
    loader = TimescaleDBLoader()
    end = pd.Timestamp.utcnow().normalize()
    start_all = end - pd.Timedelta(days=int(years * 365))

    for symbol in symbols:
        if '/' in symbol:
            logger.info(f"{symbol}: crypto - use backfill_crypto, skipped")
            continue
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
    logger.info("===== BACKFILL COMPLETE -> backfill_crypto, resample_to_1h, "
                "build_memory, update_qdrant_payloads =====")


if __name__ == "__main__":
    main()
