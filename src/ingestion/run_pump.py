#!/usr/bin/env python3
"""
src/ingestion/run_pump.py
Continuous ingestion: Polygon -> indicators v2 -> TimescaleDB (+ sentiment).
Run from project root:  python -m src.ingestion.run_pump
"""
import time
from src.ingestion.fetcher import PolygonFetcher
from src.ingestion.indicator_calculator import calculate_all_indicators
from src.ingestion.loader import TimescaleDBLoader
from src.news_engine.orchestrator import NewsOrchestrator
from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("IngestionPump", "logs/ingestion.log")


def main():
    logger.info("===== INGESTION PUMP START =====")
    fetcher = PolygonFetcher()
    loader = TimescaleDBLoader()
    news = NewsOrchestrator()

    for symbol in config.symbols:
        logger.info(f"Processing {symbol}...")

        df_raw = fetcher.fetch_latest_intraday(symbol, multiplier=config.BAR_MINUTES)
        if df_raw.empty:
            logger.warning(f"{symbol}: no raw data")
            continue

        df = calculate_all_indicators(df_raw)
        if df.empty:
            logger.warning(f"{symbol}: no indicators (not enough bars)")
            continue

        df['sentiment_score'] = news.get_sentiment_for_symbol(symbol)

        loader.insert_market_data(df, symbol)
        loader.insert_feature_cache(df, symbol)
        time.sleep(0.5)

    loader.close()
    logger.info("===== INGESTION PUMP COMPLETE =====")


if __name__ == "__main__":
    main()
