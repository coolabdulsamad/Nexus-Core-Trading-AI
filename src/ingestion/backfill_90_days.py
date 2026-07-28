#!/usr/bin/env python3
"""
src/ingestion/backfill_90_days.py
Backfill the last 90 days of 5-minute data for all symbols.
"""
from fetcher import PolygonFetcher
from indicator_calculator import calculate_all_indicators
from loader import TimescaleDBLoader
from config.settings import config
from utils.logger import setup_logger
import time
from datetime import datetime, timedelta
from src.news_engine.orchestrator import NewsOrchestrator

logger = setup_logger("Backfill90Days", "logs/backfill.log")

def main():
    logger.info("===== STARTING 90-DAY BACKFILL =====")
    
    fetcher = PolygonFetcher()
    loader = TimescaleDBLoader()
    news_orchestrator = NewsOrchestrator()  # For sentiment caching (will be called per day)
    
    symbols = config.symbols
    
    # Define date range: 90 days ago to yesterday (or today)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    
    logger.info(f"Backfilling from {start_date} to {end_date}")
    
    for symbol in symbols:
        logger.info(f"Processing {symbol}...")
        
        # 1. Fetch raw data for the full 90-day range
        df_raw = fetcher.fetch_historical_bars(symbol, start_date, end_date, "minute", 5)
        if df_raw.empty:
            logger.warning(f"Skipping {symbol} - No raw data.")
            continue
        
        # 2. Calculate Technical Indicators
        df_enriched = calculate_all_indicators(df_raw)
        if df_enriched.empty:
            logger.warning(f"Skipping {symbol} - No indicators calculated.")
            continue
        
        # 3. Get a single sentiment score for the entire period (using the latest date)
        #    Note: This is a simplification. For a proper backfill, we'd fetch sentiment per day.
        #    But for now, we use the latest sentiment as a placeholder. A better approach is to
        #    use historical news data, but that's beyond this scope.
        #    We'll set sentiment to 0.0 for historical periods where we don't have it.
        #    For a realistic backtest, you'd need historical news datasets.
        #    For now, we'll use the latest sentiment or default to 0.0.
        #    Actually, our cached orchestrator will fetch latest news. We'll just use that.
        sentiment_score = news_orchestrator.get_sentiment_for_symbol(symbol)
        df_enriched['sentiment_score'] = sentiment_score
        logger.info(f"Sentiment score for {symbol}: {sentiment_score}")
        
        # 4. Load into TimescaleDB (this will update existing rows)
        loader.insert_market_data(df_enriched, symbol)
        loader.insert_feature_cache(df_enriched, symbol)
        
        # Sleep to avoid rate limits
        time.sleep(1)
    
    loader.close()
    logger.info("===== 90-DAY BACKFILL COMPLETED =====")

if __name__ == "__main__":
    main()