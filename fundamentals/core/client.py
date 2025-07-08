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
            self.timestamps = [ts for ts in self.timestamps if now - ts < self.period]
            if len(self.timestamps) >= self.max_calls:
                sleep_time = (self.timestamps[0] + self.period) - now
                if sleep_time > 0:
                    logging.info(f"Rate limit reached. Sleeping for {sleep_time:.2f}s")
                    time.sleep(sleep_time)
            self.timestamps.append(time.time())

class FMPClient:
    """A client for fetching fundamentals data from FMP."""
    BASE_URL = "https://financialmodelingprep.com/api/v3"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("FMP API key is required.")
        self.api_key = api_key
        self.rate_limiter = RateLimiter(max_calls=45, period=1.0)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
    def _make_request(self, endpoint: str, params: dict) -> list:
        """Makes a rate-limited and retriable request to the FMP API."""
        self.rate_limiter.acquire()
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            logging.error(f"HTTP Error for {url}: {e}")
            # For 4xx errors, especially 404 Not Found, don't retry.
            if 400 <= e.response.status_code < 500:
                return []
            raise
        except Exception as e:
            logging.error(f"Request failed for {url}: {e}")
            raise

    def get_latest_quarter_end_date(self, ticker: str) -> str | None:
        """Gets the most recent quarter-end date string for a ticker."""
        endpoint = f"income-statement/{ticker}"
        params = {"period": "quarter", "limit": 1, "apikey": self.api_key}
        try:
            data = self._make_request(endpoint, params)
            if isinstance(data, list) and data and data[0].get("date"):
                return data[0]["date"]
            logging.warning(f"{ticker}: Could not parse latest quarter date from response: {data}")
            return None
        except Exception as e:
            logging.warning(f"{ticker}: Could not fetch latest quarter date: {e}")
            return None

    def get_financial_data(self, ticker: str, endpoint: str, limit: int) -> list[dict]:
        """Fetches a list of quarterly financial data records."""
        params = {"period": "quarter", "limit": limit, "apikey": self.api_key}
        data = self._make_request(f"{endpoint}/{ticker}", params)
        return data if isinstance(data, list) else []