"""
src/news_engine/orchestrator.py
UPDATED: Uses real news sentiment (FinBERT) if enabled, falls back to synthetic.
"""
import pandas as pd
from datetime import datetime
import hashlib
from config.settings import config
from src.news_engine.scraper import NewsScraper
from src.news_engine.sentiment import SentimentAnalyzer
from utils.logger import setup_logger

logger = setup_logger("NewsOrchestrator", "logs/news.log")

class NewsOrchestrator:
    def __init__(self):
        self.use_real = config.USE_REAL_SENTIMENT
        self.scraper = NewsScraper() if self.use_real else None
        self.analyzer = SentimentAnalyzer() if self.use_real else None
        self._cache = {}

    def get_sentiment_for_symbol(self, symbol: str, timestamp=None) -> float:
        """
        Returns a sentiment score for a given symbol and timestamp.
        If real sentiment is enabled and headlines exist, uses FinBERT.
        Otherwise falls back to deterministic synthetic value.
        """
        if timestamp is None:
            date_str = datetime.now().date().isoformat()
        else:
            if hasattr(timestamp, 'date'):
                date_str = timestamp.date().isoformat()
            else:
                date_str = pd.to_datetime(timestamp).date().isoformat()
        
        key = (symbol, date_str)
        if key in self._cache:
            return self._cache[key]
        
        if self.use_real and self.scraper and self.analyzer:
            try:
                headlines = self.scraper.fetch_headlines(symbol, days_back=1)
                if headlines:
                    score = self.analyzer.get_aggregate_sentiment(headlines)
                    self._cache[key] = score
                    logger.info(f"Real sentiment for {symbol} on {date_str}: {score}")
                    return score
                else:
                    logger.warning(f"No headlines for {symbol}, using synthetic fallback.")
            except Exception as e:
                logger.error(f"Real sentiment failed: {e}, using synthetic.")
        
        # Synthetic sentiment based on date hash
        hash_val = int(hashlib.md5(f"{symbol}_{date_str}".encode()).hexdigest(), 16) % 100
        synthetic_sentiment = round(((hash_val / 100) * 1.6) - 0.8, 4)
        self._cache[key] = synthetic_sentiment
        
        logger.info(f"Synthetic sentiment for {symbol} on {date_str}: {synthetic_sentiment}")
        return synthetic_sentiment