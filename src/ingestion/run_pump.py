#!/usr/bin/env python3
"""
src/ingestion/run_pump.py
UPDATED: Now fetches news sentiment for each symbol and injects it into the feature cache.
"""
from fetcher import PolygonFetcher
from indicator_calculator import calculate_all_indicators
from loader import TimescaleDBLoader
from config.settings import config
from utils.logger import setup_logger
from src.news_engine.orchestrator import NewsOrchestrator
import time
from datetime import datetime, timedelta

logger = setup_logger("IngestionPump", "logs/ingestion.log")

def main():
    logger.info("===== STARTING DATA INGESTION PUMP (WITH SENTIMENT) =====")
    
    fetcher = PolygonFetcher()
    loader = TimescaleDBLoader()
    news_orchestrator = NewsOrchestrator()  # Initialize FinBERT once
    
    symbols = config.symbols  
    
    for symbol in symbols:
        logger.info(f"Processing {symbol}...")
        
        # 1. Fetch raw market data
        df_raw = fetcher.fetch_latest_intraday(symbol, multiplier=5)
        if df_raw.empty:
            logger.warning(f"Skipping {symbol} - No raw data.")
            continue
        
        # 2. Calculate Technical Indicators
        df_enriched = calculate_all_indicators(df_raw)
        if df_enriched.empty:
            logger.warning(f"Skipping {symbol} - No indicators calculated.")
            continue
        
        # 3. FETCH NEWS SENTIMENT FOR THIS SYMBOL (NEW STEP)
        sentiment_score = news_orchestrator.get_sentiment_for_symbol(symbol)
        # Add the sentiment score to EVERY row of the DataFrame for this symbol
        df_enriched['sentiment_score'] = sentiment_score
        
        logger.info(f"Sentiment score for {symbol}: {sentiment_score}")
        
        # 4. Load into TimescaleDB
        loader.insert_market_data(df_enriched, symbol)
        loader.insert_feature_cache(df_enriched, symbol)
        
        # Sleep a tiny bit to avoid rate limiting
        time.sleep(0.5)
    
    loader.close()
    logger.info("===== INGESTION PUMP COMPLETED =====")

if __name__ == "__main__":
    main()