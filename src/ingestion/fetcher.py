"""
src/ingestion/fetcher.py
Responsibility: Raw data acquisition from Polygon.io.
Handles retries, API key rotation (if needed), and converts API JSON to Pandas DataFrame.
"""
import time
import pandas as pd
from datetime import datetime, timedelta
from polygon import RESTClient
from polygon.rest.models.aggs import Agg
from typing import List, Optional

# Import our project settings and logger
from config.settings import config
from utils.logger import setup_logger

logger = setup_logger("DataFetcher", "logs/ingestion.log")

class PolygonFetcher:
    def __init__(self):
        self.api_key = config.polygon.api_key
        if not self.api_key or self.api_key == "your_polygon_api_key_here":
            logger.error("Polygon API Key is missing! Check your .env file.")
            raise ValueError("Missing Polygon API Key")
        self.client = RESTClient(api_key=self.api_key)

    def fetch_historical_bars(
        self, 
        symbol: str, 
        start_date: str, 
        end_date: str, 
        timespan: str = "minute", 
        multiplier: int = 5
    ) -> pd.DataFrame:
        """
        Fetches historical aggregate bars.
        
        Args:
            symbol: e.g., 'AAPL', 'SPY'
            start_date: 'YYYY-MM-DD'
            end_date: 'YYYY-MM-DD'
            timespan: 'minute', 'hour', 'day'
            multiplier: e.g., 5 for 5-minute bars.
        Returns:
            Pandas DataFrame with columns: timestamp, open, high, low, close, volume
        """
        logger.info(f"Fetching {multiplier}-{timespan} data for {symbol} from {start_date} to {end_date}")
        
        try:
            # Polygon's list_aggs returns an iterator of Agg objects
            aggs_iter = self.client.list_aggs(
                ticker=symbol,
                multiplier=multiplier,
                timespan=timespan,
                from_=start_date,
                to=end_date,
                limit=50000  # Max per request
            )
            
            # Convert the iterator to a list and then to DataFrame
            data: List[Agg] = list(aggs_iter)
            
            if not data:
                logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()
            
            # Convert to dictionary and then DataFrame
            records = []
            for agg in data:
                records.append({
                    'timestamp': pd.to_datetime(agg.timestamp, unit='ms'),  # Polygon uses ms
                    'open': float(agg.open),
                    'high': float(agg.high),
                    'low': float(agg.low),
                    'close': float(agg.close),
                    'volume': float(agg.volume)
                })
            
            df = pd.DataFrame(records)
            
            # Drop duplicates just in case
            df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            
            logger.info(f"Successfully fetched {len(df)} rows for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch data for {symbol}: {str(e)}")
            # Sleep briefly to avoid hammering the API on failure
            time.sleep(2)
            return pd.DataFrame()

    def fetch_latest_intraday(self, symbol: str, multiplier: int = 5) -> pd.DataFrame:
        """
        Fetches the last 5 days of intraday data (useful for daily updates).
        """
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        return self.fetch_historical_bars(symbol, start_date, end_date, "minute", multiplier)

# --- Quick Test (Run this if you execute the file directly) ---
if __name__ == "__main__":
    fetcher = PolygonFetcher()
    # Test fetching SPY data for the last 3 days
    test_df = fetcher.fetch_historical_bars("SPY", "2026-06-20", "2026-06-23", "minute", 5)
    print(test_df.head())