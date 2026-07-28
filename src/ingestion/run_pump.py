#!/usr/bin/env python3
"""
src/ingestion/run_pump.py
Continuous ingestion: Polygon -> indicators v2 -> TimescaleDB (+ sentiment).
Universe-driven: pulls the active symbol list from the DB (stocks + crypto).
Run from project root:  python -m src.ingestion.run_pump
"""
import time
from datetime import datetime, timedelta
from src.ingestion.fetcher import PolygonFetcher
from src.ingestion.indicator_calculator import calculate_all_indicators
from src.ingestion.loader import TimescaleDBLoader
from src.news_engine.orchestrator import NewsOrchestrator
from src.universe.symbol_manager import SymbolManager
from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("IngestionPump", "logs/ingestion.log")


def main():
    logger.info("===== INGESTION PUMP START =====")
    fetcher = PolygonFetcher()
    loader = TimescaleDBLoader()
    news = NewsOrchestrator()
    mgr = SymbolManager()

    universe = mgr.list_symbols(active_only=True)
    if not universe:
        universe = [{'symbol': s, 'asset_type': 'stock'} for s in config.symbols]

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    for item in universe:
        symbol, asset_type = item['symbol'], item.get('asset_type', 'stock')
        if asset_type == 'crypto' and not config.CRYPTO_ENABLED:
            continue
        logger.info(f"Processing {symbol} ({asset_type})...")

        try:
            if asset_type == 'crypto':
                df_raw = fetcher.fetch_crypto_bars(symbol, start_date, end_date,
                                                   "minute", config.BAR_MINUTES)
            else:
                df_raw = fetcher.fetch_latest_intraday(symbol, multiplier=config.BAR_MINUTES)
        except Exception as e:
            logger.error(f"{symbol}: fetch failed: {e}")
            continue

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
