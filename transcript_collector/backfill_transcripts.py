import os
import logging
import datetime
import pandas as pd
import requests
import json # <-- ADDED: Import Python's standard JSON library
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

from google.cloud import bigquery, storage
from tenacity import retry, stop_after_attempt, wait_exponential

# --- Configuration ---
# ---!!!--- Make sure these values are correct ---!!!---
PROJECT_ID = "profitscout-lx6bb"
GCS_BUCKET_NAME = "profit-scout-data"
BIGQUERY_TABLE_ID = "profitscout-lx6bb.profit_scout.stock_metadata"
GCS_OUTPUT_FOLDER = "earnings-call-transcripts/"
MAX_WORKERS = 10 # Adjust based on your machine's capability and API rate limits

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- API Client and Rate Limiter (self-contained for portability) ---

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
    """A client for fetching transcript data from FMP."""
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
        except requests.RequestException as e:
            logging.error(f"Request failed for {url}: {e}")
            raise

    def fetch_transcript(self, ticker: str, year: int, quarter: int) -> dict | None:
        """Fetches an earnings call transcript for a specific year and quarter."""
        params = {"year": year, "quarter": quarter, "apikey": self.api_key}
        data = self._make_request(f"earning_call_transcript/{ticker}", params)
        return data[0] if isinstance(data, list) and data else None

# --- Core Logic ---

def get_records_from_bq(client: bigquery.Client) -> list:
    """Fetches all ticker/date records from the metadata table."""
    logging.info(f"Querying BigQuery table: {BIGQUERY_TABLE_ID}")
    query = f"""
        SELECT ticker, quarter_end_date
        FROM `{BIGQUERY_TABLE_ID}`
        WHERE ticker IS NOT NULL AND quarter_end_date IS NOT NULL
        ORDER BY ticker, quarter_end_date
    """
    try:
        query_job = client.query(query)
        records = list(query_job.result())
        logging.info(f"Found {len(records)} records in BigQuery.")
        return records
    except Exception as e:
        logging.critical(f"Failed to query BigQuery: {e}")
        return []

def process_record(record, fmp_client: FMPClient, storage_client: storage.Client) -> str:
    """
    Processes a single record: checks if the transcript exists, and if not,
    fetches and uploads it.
    """
    ticker = record.ticker
    quarter_end_date = record.quarter_end_date

    # Construct the expected GCS blob path
    blob_path = f"{GCS_OUTPUT_FOLDER}{ticker}_{quarter_end_date.isoformat()}.json"
    
    # 1. Check if the blob already exists in GCS
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(blob_path)
    if blob.exists():
        return f"EXISTS: {ticker} on {quarter_end_date}"

    # 2. If it doesn't exist, fetch it
    logging.info(f"MISSING: Attempting to fetch for {ticker} on {quarter_end_date}")
    
    # Derive year and quarter from the date.
    dt = pd.to_datetime(quarter_end_date)
    year, quarter = dt.year, dt.quarter

    try:
        transcript_data = fmp_client.fetch_transcript(ticker, year, quarter)
        if not (transcript_data and transcript_data.get("content")):
            return f"NO_CONTENT: {ticker} for {year} Q{quarter}"

        # 3. Upload the fetched data to GCS
        # <-- FIXED: Use the standard json.dumps() to convert the dict to a string
        blob.upload_from_string(json.dumps(transcript_data, indent=2), content_type="application/json")
        return f"SUCCESS: Fetched and uploaded for {ticker} on {quarter_end_date}"

    except Exception as e:
        logging.error(f"Failed to process {ticker} for {quarter_end_date}: {e}")
        return f"ERROR: {ticker} on {quarter_end_date}"

# --- Main Execution ---

if __name__ == "__main__":
    logging.info("--- Starting Transcript Backfill Script ---")

    # Get FMP API Key from environment variable
    fmp_api_key = os.getenv("FMP_API_KEY")
    if not fmp_api_key:
        raise ValueError("FMP_API_KEY environment variable not set.")

    # Initialize clients
    bq_client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)
    fmp_client = FMPClient(api_key=fmp_api_key)

    # Get all records from BigQuery
    all_records = get_records_from_bq(bq_client)

    if not all_records:
        logging.info("No records to process. Exiting.")
    else:
        logging.info(f"Starting to process {len(all_records)} records with {MAX_WORKERS} workers...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_record = {executor.submit(process_record, rec, fmp_client, storage_client): rec for rec in all_records}
            
            for future in as_completed(future_to_record):
                record = future_to_record[future]
                try:
                    result_message = future.result()
                    if "SUCCESS" in result_message or "MISSING" in result_message:
                        logging.info(result_message)
                    elif "EXISTS" not in result_message:
                        logging.warning(result_message)
                except Exception as exc:
                    logging.error(f"Record {record.ticker}/{record.quarter_end_date} generated an exception: {exc}")
    
    logging.info("--- Transcript Backfill Script Finished ---")