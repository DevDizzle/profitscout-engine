# ingestion/core/clients/sec_api_client.py
import logging

from sec_api import ExtractorApi, QueryApi
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential


def _is_rate_limit_error(e):
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
        reraise=True,
    )
    def _get_latest_filing(self, query: dict) -> dict | None:
        response = self.query_api.get_filings(query)
        return response["filings"][0] if response.get("filings") else None

    def get_latest_filings(self, ticker: str) -> dict[str, dict]:
        filings = {}
        annual_query = {
            "query": {
                "query_string": {
                    "query": f'ticker:"{ticker}" AND (formType:"10-K" OR formType:"20-F" OR formType:"40-F" OR formType:"10-KT")'
                }
            },
            "from": "0",
            "size": "1",
            "sort": [{"filedAt": {"order": "desc"}}],
        }
        filings["annual"] = self._get_latest_filing(annual_query)
        quarterly_query = {
            "query": {
                "query_string": {
                    "query": f'ticker:"{ticker}" AND (formType:"10-Q" OR formType:"10-QT")'
                }
            },
            "from": "0",
            "size": "1",
            "sort": [{"filedAt": {"order": "desc"}}],
        }
        filings["quarterly"] = self._get_latest_filing(quarterly_query)
        return filings

    @retry(
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(3),
        retry=retry_if_exception(_is_rate_limit_error),
        reraise=True,
    )
    def extract_section(self, filing_url: str, section_key: str) -> str:
        try:
            return self.extractor_api.get_section(filing_url, section_key, "text")
        except Exception as e:
            if "not supported" in str(e) or "not found" in str(e):
                logging.warning(f"Section {section_key} not found for {filing_url}")
                return ""
            raise
