import concurrent.futures
import datetime
import logging
import os
import time
from threading import Lock

import pandas as pd
import requests
from google.cloud import bigquery, storage

# ==============================================================================
# CONFIGURATION
# ==============================================================================
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "price_data")
TICKER_FILE_PATH = "tickerlist.txt"
DEFAULT_START_DATE = datetime.date(2022, 1, 1)
MAX_WORKERS = 24

def _get_secret(secret_id: str) -> str | None:
    """Reads a secret from the filesystem, as mounted by Google Cloud Functions."""
    secret_path = f"/secrets/{secret_id}"
    try:
        with open(secret_path, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        logging.error(f"Secret file not found at {secret_path}. Ensure the secret is mounted correctly in your deployment.")
        # For local testing, you can fall back to an environment variable
        fallback_key = os.getenv(secret_id)
        if fallback_key:
            logging.warning(f"Falling back to environment variable for {secret_id}. This should not happen in production.")
            return fallback_key
        return None

# Retrieve the FMP API Key from Secret Manager
FMP_API_KEY = _get_secret("FMP_API_KEY")

# ==============================================================================
# SETUP LOGGING AND API CLIENTS
# ==============================================================================
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
bq_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)

# (The rest of the helper functions: RateLimiter, get_tickers, get_start_dates, fetch_prices_for_ticker remain unchanged from the previous version)

# ==============================================================================
# RATE LIMITER
# ==============================================================================
class RateLimiter:
    """A simple thread-safe rate limiter."""
    def __init__(self, max_calls_per_period=50, period=1.0):
        self.max_calls = max_calls_per_period
        self.period = period
        self.calls_timestamps = []
        self.lock = Lock()

    def acquire(self):
        with self.lock:
            now = time.time()
            self.calls_timestamps = [ts for ts in self.calls_timestamps if now - ts < self.period]
            if len(self.calls_timestamps) >= self.max_calls:
                sleep_time = (self.calls_timestamps[0] + self.period) - now
                logging.info(f"Rate limit reached. Sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time)
            self.calls_timestamps.append(time.time())

rate_limiter = RateLimiter()


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def get_tickers() -> list[str]:
    """Loads the authoritative ticker list from a simple text file in GCS."""
    try:
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(TICKER_FILE_PATH)
        if not blob.exists():
            logging.error(f"FATAL: Ticker file not found at gs://{GCS_BUCKET_NAME}/{TICKER_FILE_PATH}")
            return []
        
        ticker_data = blob.download_as_text(encoding="utf-8")
        tickers = [
            line.strip().upper()
            for line in ticker_data.strip().split("\n")
            if line.strip()
        ]
        logging.info(f"Loaded {len(tickers)} tickers from gs://{GCS_BUCKET_NAME}/{TICKER_FILE_PATH}")
        return tickers
    except Exception as e:
        logging.error(f"FATAL: Could not read ticker file from GCS: {e}")
        return []


def get_start_dates(all_tickers: list[str]) -> dict:
    """Queries BigQuery once to get the last recorded date for all tickers."""
    logging.info("Querying BigQuery to get max dates for all existing tickers...")
    table_ref = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
    try:
        query = f"""
            SELECT ticker, MAX(date) AS max_date
            FROM `{table_ref}`
            GROUP BY ticker
        """
        results = bq_client.query(query).result()
        max_dates = {row["ticker"]: row["max_date"].date() for row in results if row["max_date"]}
        logging.info(f"Found max dates for {len(max_dates)} tickers in BigQuery.")
    except Exception as e:
        logging.warning(f"Could not query max dates from BigQuery (this is normal if table is new): {e}")
        max_dates = {}

    start_dates = {}
    for ticker in all_tickers:
        if ticker in max_dates:
            start_dates[ticker] = max_dates[ticker] + datetime.timedelta(days=1)
        else:
            start_dates[ticker] = DEFAULT_START_DATE
    return start_dates


def fetch_prices_for_ticker(ticker: str, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    """Fetches historical price data for a single ticker from the FMP API."""
    if start_date > end_date:
        logging.info(f"{ticker}: Already up-to-date.")
        return pd.DataFrame()

    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?from={start_str}&to={end_str}&apikey={FMP_API_KEY}"
    
    rate_limiter.acquire()
    
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        hist = data.get("historical", [])
        if not hist:
            logging.warning(f"{ticker}: No historical data returned from API for date range {start_str} to {end_str}.")
            return pd.DataFrame()

        df = pd.DataFrame(hist)
        df = df.rename(columns={"adjClose": "adj_close"})
        df["ticker"] = ticker
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.date
        
        required_cols = ["ticker", "date", "open", "high", "low", "adj_close", "volume"]
        df = df[[col for col in required_cols if col in df.columns]]

        for col in ["open", "high", "low", "adj_close", "volume"]:
             if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna()

        logging.info(f"{ticker}: Fetched {len(df)} new rows from {start_str} to {end_str}.")
        return df
    except requests.HTTPError as http_err:
        logging.error(f"{ticker}: HTTP error fetching data: {http_err}")
    except Exception as e:
        logging.error(f"{ticker}: An unexpected error occurred: {e}")
        
    return pd.DataFrame()


# ==============================================================================
# MAIN CLOUD FUNCTION ENTRYPOINT
# ==============================================================================
def populate_price_data(request) -> tuple[str, int]:
    """
    HTTP-triggered Cloud Function to update the BigQuery price table.
    Orchestrates the entire batch update process.
    """
    if not FMP_API_KEY:
        logging.critical("FMP_API_KEY secret is not configured. Terminating function.")
        return "Server configuration error.", 500

    logging.info("=== Starting Daily Price Update Batch Job ===")
    
    all_tickers = get_tickers()
    if not all_tickers:
        logging.error("No tickers found. Exiting job.")
        return "No tickers found in tickerlist.txt.", 200

    today = datetime.date.today()
    start_dates = get_start_dates(all_tickers)

    all_dataframes = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {
            executor.submit(fetch_prices_for_ticker, ticker, start_dates.get(ticker, DEFAULT_START_DATE), today): ticker
            for ticker in all_tickers
        }
        for future in concurrent.futures.as_completed(future_to_ticker):
            try:
                result_df = future.result()
                if not result_df.empty:
                    all_dataframes.append(result_df)
            except Exception as e:
                ticker = future_to_ticker[future]
                logging.error(f"An exception was raised for ticker {ticker} during fetch: {e}")

    if not all_dataframes:
        logging.info("No new price data to load. Job complete.")
        return "No new price data to load.", 200

    final_df = pd.concat(all_dataframes, ignore_index=True)
    logging.info(f"Prepared a total of {len(final_df)} new rows to load into BigQuery.")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("adj_close", "FLOAT"),
            bigquery.SchemaField("volume", "INTEGER"),
        ],
    )

    try:
        table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
        load_job = bq_client.load_table_from_dataframe(
            final_df, table_id, job_config=job_config
        )
        load_job.result()
        
        logging.info(f"Successfully loaded {load_job.output_rows} rows into {table_id}.")
        logging.info("=== Daily Price Update Batch Job Complete ===")
        return f"Successfully loaded {load_job.output_rows} rows.", 200
        
    except Exception as e:
        logging.error(f"FATAL: Failed to load data into BigQuery: {e}")
        return "Failed to load data into BigQuery.", 500
