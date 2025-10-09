# ingestion/core/clients/fmp_client.py
import logging
import time
from threading import Lock
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
import pandas as pd
import datetime

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
    """A shared client for fetching data from the Financial Modeling Prep API."""
    BASE_URL = "https://financialmodelingprep.com/api/v3"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("FMP API key is required.")
        self.api_key = api_key
        self.rate_limiter = RateLimiter(max_calls=45, period=1.0)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
    def _make_request(self, endpoint: str, params: dict) -> list | dict:
        """Makes a rate-limited and retriable request to the FMP API."""
        self.rate_limiter.acquire()
        url = f"{self.BASE_URL}/{endpoint}"
        params['apikey'] = self.api_key
        try:
            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            logging.error(f"HTTP Error for {url}: {e}")
            if 400 <= e.response.status_code < 500: return []
            raise
        except Exception as e:
            logging.error(f"Request failed for {url}: {e}")
            raise

    def get_latest_quarter_end_date(self, ticker: str) -> str | None:
        """Gets the most recent quarter-end date string for a ticker."""
        params = {"period": "quarter", "limit": 1}
        try:
            data = self._make_request(f"income-statement/{ticker}", params)
            if isinstance(data, list) and data and data[0].get("date"):
                return data[0]["date"]
            return None
        except Exception as e:
            logging.warning(f"{ticker}: Could not fetch latest quarter date: {e}")
            return None

    def get_financial_data(self, ticker: str, endpoint: str, limit: int) -> list[dict]:
        """Fetches a list of quarterly financial data records (for fundamentals)."""
        params = {"period": "quarter", "limit": limit}
        data = self._make_request(f"{endpoint}/{ticker}", params)
        return data if isinstance(data, list) else []

    def get_financial_statements(self, ticker: str, limit: int) -> dict[str, list]:
        """Fetches and returns all three core financial statements."""
        statements = {}
        types = {
            "income": "income-statement",
            "balance": "balance-sheet-statement",
            "cashflow": "cash-flow-statement"
        }
        for key, statement_type in types.items():
            params = {"period": "quarter", "limit": limit}
            statements[key] = self._make_request(f"{statement_type}/{ticker}", params=params)
        return statements

    def fetch_90_day_prices(self, ticker: str) -> list[dict] | None:
        """Fetches the last 90 days of full price data for a ticker."""
        params = {"timeseries": 90}
        data = self._make_request(f"historical-price-full/{ticker}", params=params)
        return data.get("historical") if isinstance(data, dict) else None

    def fetch_prices_for_populator(self, ticker: str, start: datetime.date, end: datetime.date) -> pd.DataFrame:
        """Fetches historical price data for a single ticker for the populator service."""
        if start > end:
            return pd.DataFrame()

        url = (f"{self.BASE_URL}/historical-price-full/{ticker}"
               f"?from={start.isoformat()}&to={end.isoformat()}&apikey={self.api_key}")
        self.rate_limiter.acquire()

        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.json().get("historical", [])
            if not data: return pd.DataFrame()

            df = pd.DataFrame(data).rename(columns={"adjClose": "adj_close"})
            df["ticker"] = ticker
            df["date"] = pd.to_datetime(df["date"]).dt.date
            schema_columns = ["ticker", "date", "open", "high", "low", "adj_close", "volume"]
            return df.get(schema_columns, pd.DataFrame())

        except requests.RequestException as e:
            logging.error(f"Failed to fetch prices for {ticker}: {e}")
            return pd.DataFrame()

    def fetch_calendar(self, endpoint: str, start: datetime.date, end: datetime.date) -> list[dict]:
        """Fetches calendar data from FMP for a date range."""
        params = {"from": start.isoformat(), "to": end.isoformat()}
        data = self._make_request(endpoint, params=params)
        return data if isinstance(data, list) else []

    def fetch_transcript(self, ticker: str, year: int, quarter: int) -> dict | None:
        """ Fetches a specific earnings call transcript."""
        params = {'quarter': quarter, 'year': year}
        data = self._make_request(f"earning_call_transcript/{ticker}", params=params)
        return data[0] if isinstance(data, list) and data else None