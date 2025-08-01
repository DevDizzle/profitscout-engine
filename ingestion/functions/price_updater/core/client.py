import logging
import time
from threading import Lock
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

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
                if sleep_time > 0:
                    logging.info(f"Rate limit reached. Sleeping for {sleep_time:.2f}s")
                    time.sleep(sleep_time)
            self.timestamps.append(time.time())

class FMPClient:
    """A client for fetching price data from FMP."""
    BASE_URL = "https://financialmodelingprep.com/api/v3"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("FMP API key is required.")
        self.api_key = api_key
        self.rate_limiter = RateLimiter(max_calls=45, period=1.0)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
    def fetch_90_day_prices(self, ticker: str) -> list[dict] | None:
        """Fetches the last 90 days of full price data for a ticker."""
        self.rate_limiter.acquire()
        url = f"{self.BASE_URL}/historical-price-full/{ticker}"
        params = {"timeseries": 90, "apikey": self.api_key}
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            # The API returns a dict with a 'historical' key
            return data.get("historical")
        except requests.RequestException as e:
            logging.error(f"{ticker}: Could not fetch 90-day prices: {e}")
            return None