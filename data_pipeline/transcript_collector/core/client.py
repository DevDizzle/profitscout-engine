import logging
import time
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from .. import config

class RateLimiter:
    """A simple rate limiter to manage requests per second."""
    def __init__(self, requests_per_second: int):
        self.interval = 1.0 / requests_per_second
        self.last_request_time = 0

    def is_limited(self) -> bool:
        """Check if a new request should be delayed."""
        return time.monotonic() - self.last_request_time < self.interval
    
    def get_delay(self) -> float:
        """Calculate the required delay time."""
        return self.interval - (time.monotonic() - self.last_request_time)

    def update_request_time(self):
        """Update the time of the last request."""
        self.last_request_time = time.monotonic()


class FMPClient:
    """A client for interacting with the Financial Modeling Prep (FMP) API."""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("FMP API key is required.")
        self.api_key = api_key
        self.base_url = "https://financialmodelingprep.com/api/v3"
        self.rate_limiter = RateLimiter(requests_per_second=25)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _make_request(self, endpoint: str) -> dict | list | None:
        """Makes a request to the FMP API with rate limiting and retries."""
        url = f"{self.base_url}{endpoint}&apikey={self.api_key}"
        
        if self.rate_limiter.is_limited():
            delay = self.rate_limiter.get_delay()
            logging.info(f"Rate limit reached. Sleeping for {delay:.2f}s")
            time.sleep(delay)

        self.rate_limiter.update_request_time()
        
        import requests 
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()

    def fetch_transcript(self, ticker: str, year: int, quarter: int) -> dict | None:
        """
        Fetches the content of a single earnings call transcript for a given
        ticker, year, and quarter.
        """
        try:
            all_transcripts = self._make_request(f"/earning_call_transcript/{ticker}?")

            if not all_transcripts or not isinstance(all_transcripts, list):
                return None

            for transcript in all_transcripts:
                if transcript.get('year') == year and transcript.get('quarter') == quarter:
                    specific_transcript_data = self._make_request(
                        f"/earning_call_transcript/{ticker}?quarter={quarter}&year={year}"
                    )
                    if specific_transcript_data and isinstance(specific_transcript_data, list):
                        return specific_transcript_data[0] 

            return None
            
        except RetryError as e:
            logging.error(f"API call failed after multiple retries for {ticker} {year} Q{quarter}: {e}")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred fetching transcript for {ticker} {year} Q{quarter}: {e}")
            return None