"""
src/news_engine/scraper.py
Responsibility: Fetch financial news headlines from NewsAPI.
"""
import requests
import os
from datetime import datetime, timedelta
from typing import List, Dict
from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("NewsScraper", "logs/news.log")

class NewsScraper:
    def __init__(self):
        # You will need to get a FREE API key from https://newsapi.org/
        # Add it to your .env file: NEWS_API_KEY=your_key_here
        self.api_key = os.getenv("NEWS_API_KEY", "")
        if not self.api_key:
            logger.warning("NEWS_API_KEY not found in .env. News scraping will return empty.")
        self.base_url = "https://newsapi.org/v2/everything"

    def fetch_headlines(self, symbol: str, days_back: int = 1) -> List[str]:
        """
        Fetches the top headlines for a given symbol (e.g., 'AAPL' or 'SPY').
        """
        if not self.api_key:
            return []

        # Calculate date range (last 24 hours)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        params = {
            "q": symbol,  # Search query
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d"),
            "language": "en",
            "sortBy": "relevancy",
            "apiKey": self.api_key,
            "pageSize": 10  # Max 10 headlines per request (free tier)
        }

        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get("status") != "ok":
                logger.warning(f"NewsAPI error for {symbol}: {data.get('message')}")
                return []

            headlines = [article["title"] for article in data.get("articles", []) if article.get("title")]
            logger.info(f"Fetched {len(headlines)} headlines for {symbol}")
            return headlines

        except Exception as e:
            logger.error(f"Failed to fetch news for {symbol}: {str(e)}")
            return []

    def fetch_headlines_all_symbols(self, symbols=None) -> Dict[str, List[str]]:
        """Fetches headlines for all symbols in the config."""
        if symbols is None:
            symbols = config.symbols

        results = {}
        for sym in symbols:
            results[sym] = self.fetch_headlines(sym, days_back=1)
        return results
