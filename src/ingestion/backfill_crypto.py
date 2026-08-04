#!/usr/bin/env python3
"""
src/ingestion/backfill_crypto.py
Backfills years of 1-HOUR crypto bars from ALPACA into the 1h data layer
(market_data_1h + feature_cache_1h): indicators v2 + forward returns,
the same shape resample_to_1h produces for stocks.

Why Alpaca (not Polygon): the free Polygon plan carries no crypto
history, and Alpaca is the feed the live trader actually trades on -
history and live states come from the same source. Crypto trades 24/7,
so bars keep Alpaca's native :00 alignment (stocks use the :30 session
anchor - different on purpose).

Without this, crypto symbols have no crypto-specific memory: the brain
retrieves STOCK states for BTC/USD, scores conviction ~= 0 and holds
forever. This is what teaches the brain crypto.

Run from project root:
    python -m src.ingestion.backfill_crypto [years] [BTC/USD ...]
Then:
    python -m src.memory.build_memory
    python -m src.memory.update_qdrant_payloads
"""
import os
import sys
import pandas as pd
import psycopg2
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame

from config.settings import config
from src.ingestion.resample_to_1h import upsert_1h_layer
from src.utils.logger import setup_logger

logger = setup_logger("BackfillCrypto", "logs/ingestion.log")

PAGE_LIMIT = 10000
CHUNK_DAYS = 90                   # 90d x 24h = 2160 bars < one page


def fetch_crypto_1h_bars(client: CryptoHistoricalDataClient, symbol: str, days: int) -> pd.DataFrame:
    """Full window of Alpaca 1h crypto bars, chunked by date (no page_token needed)."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    frames = []
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), end)
        req = CryptoBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=TimeFrame.Hour,
            start=chunk_start, end=chunk_end,
            limit=PAGE_LIMIT,
        )
        bars = client.get_crypto_bars(req)
        df = bars.df
        if not df.empty and symbol in df.index.get_level_values(0):
            frames.append(df.xs(symbol).reset_index())
        chunk_start = chunk_end
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames).drop_duplicates(subset=['timestamp']).sort_values('timestamp')
    return out.reset_index(drop=True)


def _resolve_symbols(cli):
    if cli:
        return [s.upper() for s in cli]
    try:
        from src.universe.symbol_manager import SymbolManager
        rows = SymbolManager().list_symbols(active_only=True)
        crypto = [r['symbol'] for r in rows if r.get('asset_type') == 'crypto']
        if crypto:
            return crypto
    except Exception as e:
        logger.warning(f"DB universe unavailable ({e}) -> config fallback")
    return list(config.CRYPTO_SYMBOLS)


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

    client = CryptoHistoricalDataClient(
        os.getenv("ALPACA_API_KEY"), os.getenv("ALPACA_SECRET_KEY"))
    conn = psycopg2.connect(config.database.url)

    for symbol in symbols:
        logger.info(f"===== {symbol}: backfilling {years}y of 1h bars from Alpaca =====")
        try:
            df = fetch_crypto_1h_bars(client, symbol, int(years * 365))
        except Exception as e:
            logger.error(f"{symbol}: fetch failed: {e}")
            continue
        if df.empty:
            logger.warning(f"{symbol}: no bars returned")
            continue
        n_md, n_fc = upsert_1h_layer(conn, symbol, df)
        logger.info(f"{symbol}: {n_md} 1h bars | {n_fc} feature rows")

    conn.close()
    logger.info("===== CRYPTO BACKFILL COMPLETE -> build_memory, update_qdrant_payloads =====")


if __name__ == "__main__":
    main()
