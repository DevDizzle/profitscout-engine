import logging
from sec_api import QueryApi, ExtractorApi
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception

def _is_rate_limit_error(e):
    """Check if the exception is a rate-limit error from sec-api."""
    return "429" in str(e) or "Too many requests" in str(e)

class SecApiClient:
    """A client for interacting with the sec-api.io service."""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("SEC API key is required.")
        self.query_api = QueryApi(api_key=api_key)
        self.extractor_api = ExtractorApi(api_key=api_key)

    @retry(
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(5),
        retry=retry_if_exception(_is_rate_limit_error),
        reraise=True
    )
    def _get_latest_filing(self, query: dict) -> dict | None:
        """Helper to run a query with retry logic."""
        response = self.query_api.get_filings(query)
        return response["filings"][0] if response.get("filings") else None

    def get_latest_filings(self, ticker: str) -> dict[str, dict]:
        """Gets the latest annual (10-K) and quarterly (10-Q) filings."""
        filings = {}

        # Query for latest annual filing
        annual_query = {"query": {"query_string": {
            "query": f'ticker:"{ticker}" AND (formType:"10-K" OR formType:"20-F" OR formType:"40-F" OR formType:"10-KT")'
        }}, "from": "0", "size": "1", "sort": [{"filedAt": {"order": "desc"}}]}
        filings["annual"] = self._get_latest_filing(annual_query)

        # Query for latest quarterly filing
        quarterly_query = {"query": {"query_string": {
            "query": f'ticker:"{ticker}" AND (formType:"10-Q" OR formType:"10-QT")'
        }}, "from": "0", "size": "1", "sort": [{"filedAt": {"order": "desc"}}]}
        filings["quarterly"] = self._get_latest_filing(quarterly_query)

        return filings

    @retry(
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(3),
        retry=retry_if_exception(_is_rate_limit_error),
        reraise=True
    )
    def extract_section(self, filing_url: str, section_key: str) -> str:
        """Extracts a specific section from a filing URL."""
        try:
            return self.extractor_api.get_section(filing_url, section_key, "text")
        except Exception as e:
            if "not supported" in str(e) or "not found" in str(e):
                logging.warning(f"Section {section_key} not found or supported for {filing_url}")
                return ""
            raise