"""
src/ingestion/fetcher.py
Raw data acquisition from Polygon.io (stocks + crypto).
"""
import time
import pandas as pd
from datetime import datetime, timedelta
from polygon import RESTClient
from polygon.rest.models.aggs import Agg
from typing import List

from config.settings import config
from src.utils.logger import setup_logger

logger = setup_logger("DataFetcher", "logs/ingestion.log")


class PolygonFetcher:
    def __init__(self):
        self.api_key = config.polygon.api_key
        if not self.api_key or self.api_key == "your_polygon_api_key_here":
            logger.error("Polygon API key missing! Check your .env file.")
            raise ValueError("Missing Polygon API Key")
        self.client = RESTClient(api_key=self.api_key)

    def fetch_historical_bars(self, symbol: str, start_date: str, end_date: str,
                              timespan: str = "minute", multiplier: int = 5) -> pd.DataFrame:
        logger.info(f"Fetching {multiplier}-{timespan} bars for {symbol}: {start_date} -> {end_date}")
        try:
            aggs_iter = self.client.list_aggs(
                ticker=symbol, multiplier=multiplier, timespan=timespan,
                from_=start_date, to=end_date, limit=50000)
            data: List[Agg] = list(aggs_iter)
            if not data:
                logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()

            df = pd.DataFrame([{
                'timestamp': pd.to_datetime(a.timestamp, unit='ms', utc=True),
                'open': float(a.open), 'high': float(a.high), 'low': float(a.low),
                'close': float(a.close), 'volume': float(a.volume),
                'vwap': float(a.vwap) if getattr(a, 'vwap', None) else None,
            } for a in data])
            df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            logger.info(f"Fetched {len(df)} rows for {symbol}")
            return df
        except Exception as e:
            logger.error(f"Fetch failed for {symbol}: {e}")
            time.sleep(2)
            return pd.DataFrame()

    # Mapping for crypto pairs -> Polygon tickers
    @staticmethod
    def _polygon_crypto_ticker(symbol: str) -> str:
        # "BTC/USD" -> "X:BTCUSD"
        return "X:" + symbol.replace("/", "")

    def fetch_crypto_bars(self, symbol: str, start_date: str, end_date: str,
                          timespan: str = "minute", multiplier: int = 5) -> pd.DataFrame:
        ticker = self._polygon_crypto_ticker(symbol)
        logger.info(f"Fetching crypto {symbol} as {ticker}")
        return self.fetch_historical_bars(ticker, start_date, end_date, timespan, multiplier)

    def fetch_latest_intraday(self, symbol: str, multiplier: int = 5) -> pd.DataFrame:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        return self.fetch_historical_bars(symbol, start_date, end_date, "minute", multiplier)


if __name__ == "__main__":
    f = PolygonFetcher()
    print(f.fetch_historical_bars("SPY", "2026-06-20", "2026-06-23", "minute", 5).head())
