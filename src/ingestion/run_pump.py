#!/usr/bin/env python3
"""
src/ingestion/run_pump.py
Daily freshness job for the ACTIVE (1h) data layer: recent bars ->
indicators v2 -> market_data_1h + feature_cache_1h, plus fresh FinBERT
sentiment on the last 48h of rows. The DailySelector scores symbols from
these tables, so run this once a day (before the US open is ideal):

    python -m src.ingestion.run_pump

History comes from backfill_history / backfill_crypto - this job only
maintains the recent window (40 days => full SMA-200 warm-up on 1h
bars). Stocks come from Polygon, crypto from Alpaca (the same feed the
trader uses). All writes are upserts and feature columns never get
overwritten with NULL, so it's safe to run anytime, as often as you like.
"""
import os
import time
import psycopg2
from datetime import datetime, timedelta, timezone
from alpaca.data.historical import CryptoHistoricalDataClient
from config.settings import config
from src.ingestion.fetcher import PolygonFetcher
from src.ingestion.resample_to_1h import upsert_1h_layer
from src.ingestion.backfill_crypto import fetch_crypto_1h_bars
from src.news_engine.orchestrator import NewsOrchestrator
from src.universe.symbol_manager import SymbolManager
from src.utils.logger import setup_logger

logger = setup_logger("IngestionPump", "logs/ingestion.log")

WINDOW_DAYS = 40          # SMA-200 warm-up on 1h bars (~30 trading days)
PAUSE_S = 13              # Polygon free tier ~5 req/min
SENTIMENT_HOURS = 48


def _update_sentiment(conn, symbol, score):
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE feature_cache{config.BAR_SUFFIX}
            SET sentiment_score = %s
            WHERE symbol = %s AND time_bucket >= %s
        """, (score, symbol, datetime.now(timezone.utc) - timedelta(hours=SENTIMENT_HOURS)))
    conn.commit()


def main():
    logger.info("===== INGESTION PUMP START (1h layer) =====")
    fetcher = PolygonFetcher()
    news = NewsOrchestrator()
    mgr = SymbolManager()
    conn = psycopg2.connect(config.database.url)

    universe = mgr.list_symbols(active_only=True)
    if not universe:
        universe = [{'symbol': s, 'asset_type': 'stock'} for s in config.symbols]

    crypto_client = None
    for item in universe:
        symbol, asset_type = item['symbol'], item.get('asset_type', 'stock')
        if asset_type == 'crypto' and not config.CRYPTO_ENABLED:
            continue
        logger.info(f"Processing {symbol} ({asset_type})...")

        try:
            if asset_type == 'crypto':
                if crypto_client is None:
                    crypto_client = CryptoHistoricalDataClient(
                        os.getenv("ALPACA_API_KEY"), os.getenv("ALPACA_SECRET_KEY"))
                df = fetch_crypto_1h_bars(crypto_client, symbol, WINDOW_DAYS)
            else:
                df = fetcher.fetch_latest_intraday(
                    symbol, multiplier=config.BAR_MINUTES, days=WINDOW_DAYS)
        except Exception as e:
            logger.error(f"{symbol}: fetch failed: {e}")
            continue

        if df.empty:
            logger.warning(f"{symbol}: no raw data")
            continue

        try:
            n_md, n_fc = upsert_1h_layer(conn, symbol, df)
            score = news.get_sentiment_for_symbol(symbol)
            _update_sentiment(conn, symbol, score)
            logger.info(f"{symbol}: {n_md} bars | {n_fc} features | sentiment {score:+.2f}")
        except Exception as e:
            logger.error(f"{symbol}: upsert failed: {e}")

        if asset_type != 'crypto':
            time.sleep(PAUSE_S)

    conn.close()
    logger.info("===== INGESTION PUMP COMPLETE =====")


if __name__ == "__main__":
    main()
