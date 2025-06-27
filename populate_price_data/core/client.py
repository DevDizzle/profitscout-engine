# populate_price_data/core/client.py
import logging
import time
import datetime
from threading import Lock
import pandas as pd
import requests
from config import FMP_API_KEY

class RateLimiter:
    """A simple thread-safe rate limiter."""
    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.timestamps = []
        self.lock = Lock()

    def acquire(self):
        with self.lock:
            now = time.time()
            # Remove timestamps older than the period
            self.timestamps = [ts for ts in self.timestamps if now - ts < self.period]
            if len(self.timestamps) >= self.max_calls:
                sleep_time = (self.timestamps[0] + self.period) - now
                logging.info(f"Rate limit reached. Sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time)
            self.timestamps.append(time.time())

class FMPClient:
    """A client for fetching data from the Financial Modeling Prep API."""
    BASE_URL = "https://financialmodelingprep.com/api/v3"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("FMP API key is required.")
        self.api_key = api_key
        self.rate_limiter = RateLimiter(max_calls=45, period=1.0) # Stay under 50 req/sec

    def fetch_prices(self, ticker: str, start: datetime.date, end: datetime.date) -> pd.DataFrame:
        """Fetches historical price data for a single ticker."""
        if start > end:
            return pd.DataFrame()

        url = (f"{self.BASE_URL}/historical-price-full/{ticker}"
               f"?from={start.isoformat()}&to={end.isoformat()}&apikey={self.api_key}")
        self.rate_limiter.acquire()

        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.json().get("historical", [])
            if not data:
                return pd.DataFrame()

            df = pd.DataFrame(data)
            
            # Rename adjClose to match BigQuery schema
            df = df.rename(columns={"adjClose": "adj_close"})
            
            # Add the ticker and format the date
            df["ticker"] = ticker
            df["date"] = pd.to_datetime(df["date"]).dt.date

            # Define the columns that match the BigQuery schema
            schema_columns = [
                "ticker", 
                "date", 
                "open", 
                "high", 
                "low", 
                "adj_close", 
                "volume"
            ]
            
            # Return a DataFrame with only the columns that are in the schema
            return df[schema_columns]
            

        except requests.RequestException as e:
            logging.error(f"Failed to fetch prices for {ticker}: {e}")
            return pd.DataFrame()