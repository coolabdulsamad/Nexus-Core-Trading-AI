"""
src/news_engine/orchestrator.py
FIXED: the MD5 "synthetic sentiment" is GONE. It was deterministic noise that
the brain could learn fake patterns from. Now:
- Live mode (timestamp ~ today): real FinBERT on fresh headlines if enabled.
- Historical/backtest timestamps or no headlines: neutral 0.0.
"""
from datetime import datetime
import pandas as pd
from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("NewsOrchestrator", "logs/news.log")


class NewsOrchestrator:
    def __init__(self):
        self.use_real = config.USE_REAL_SENTIMENT
        self.scraper = None
        self.analyzer = None
        if self.use_real:
            try:
                from src.news_engine.scraper import NewsScraper
                from src.news_engine.sentiment import SentimentAnalyzer
                self.scraper = NewsScraper()
                self.analyzer = SentimentAnalyzer()
            except Exception as e:
                logger.error(f"Real sentiment unavailable ({e}); using neutral 0.0")
                self.use_real = False
        self._cache = {}

    def get_sentiment_for_symbol(self, symbol: str, timestamp=None) -> float:
        if timestamp is None:
            ts = pd.Timestamp.now(tz='UTC')
        else:
            ts = pd.to_datetime(timestamp)
            if ts.tzinfo is None:
                ts = ts.tz_localize('UTC')

        date_str = ts.date().isoformat()
        today_str = datetime.utcnow().date().isoformat()

        # Historical bar -> never fetch today's news for it (look-ahead guard)
        if date_str != today_str:
            return 0.0

        key = (symbol, date_str)
        if key in self._cache:
            return self._cache[key]

        score = 0.0
        if self.use_real and self.scraper and self.analyzer:
            try:
                headlines = self.scraper.fetch_headlines(symbol, days_back=1)
                if headlines:
                    score = self.analyzer.get_aggregate_sentiment(headlines)
                    logger.info(f"Real sentiment {symbol} {date_str}: {score:+.3f} ({len(headlines)} headlines)")
            except Exception as e:
                logger.error(f"Sentiment failed for {symbol}: {e}")

        self._cache[key] = score
        return score
