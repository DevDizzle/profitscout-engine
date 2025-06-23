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
BATCH_SIZE = 100  # Process 100 tickers at a time to manage memory

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==============================================================================
# PriceLoader Class
# ==============================================================================
class PriceLoader:
    def __init__(self):
        """Initializes the loader, reads secrets, and creates clients."""
        self.api_key = self._get_secret("FMP_API_KEY")
        self.bq_client = bigquery.Client(project=PROJECT_ID)
        self.storage_client = storage.Client(project=PROJECT_ID)
        self.rate_limiter = self.RateLimiter()

    class RateLimiter:
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
    
    def _get_secret(self, secret_id: str) -> str | None:
        secret_path = f"/secrets/{secret_id}"
        try:
            with open(secret_path, "r") as f:
                return f.read().strip()
        except FileNotFoundError:
            logging.warning(f"Secret file not found. Falling back to env var.")
            return os.getenv(secret_id)

    def get_tickers(self) -> list[str]:
        try:
            bucket = self.storage_client.bucket(GCS_BUCKET_NAME)
            blob = bucket.blob(TICKER_FILE_PATH)
            if not blob.exists(): return []
            return [line.strip().upper() for line in blob.download_as_text(encoding="utf-8").strip().split("\n") if line.strip()]
        except Exception as e:
            logging.error(f"FATAL: Could not read ticker file: {e}")
            return []

    def get_start_dates(self, tickers_batch: list[str]) -> dict:
        table_ref = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
        try:
            # Use a BQ parameter to avoid a giant SQL string
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("tickers", "STRING", tickers_batch)
                ]
            )
            query = f"SELECT ticker, MAX(date) AS max_date FROM `{table_ref}` WHERE ticker IN UNNEST(@tickers) GROUP BY ticker"
            results = self.bq_client.query(query, job_config=job_config).result()
            max_dates = {row["ticker"]: row["max_date"].date() for row in results if row["max_date"]}
        except Exception:
            max_dates = {}
        
        return {ticker: max_dates.get(ticker, DEFAULT_START_DATE - datetime.timedelta(days=1)) + datetime.timedelta(days=1) for ticker in tickers_batch}

    def fetch_prices_for_ticker(self, ticker: str, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
        if start_date > end_date: return pd.DataFrame()
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?from={start_date.isoformat()}&to={end_date.isoformat()}&apikey={self.api_key}"
        self.rate_limiter.acquire()
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json().get("historical", [])
            if not data: return pd.DataFrame()
            
            df = pd.DataFrame(data)
            df = df.rename(columns={"adjClose": "adj_close"})
            df["ticker"], df["date"] = ticker, pd.to_datetime(df["date"]).dt.date
            required = ["ticker", "date", "open", "high", "low", "adj_close", "volume"]
            df = df[[c for c in required if c in df.columns]]
            for col in ["open", "high", "low", "adj_close", "volume"]:
                if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
            return df.dropna()
        except Exception as e:
            logging.error(f"{ticker}: Fetch failed: {e}")
            return pd.DataFrame()

    def run(self) -> tuple[str, int]:
        if not self.api_key:
            logging.critical("API key is missing.")
            return "Server configuration error", 500

        logging.info("=== Starting Price Update Batch Job ===")
        all_tickers = self.get_tickers()
        if not all_tickers: return "No tickers found.", 200

        total_rows_loaded = 0
        for i in range(0, len(all_tickers), BATCH_SIZE):
            tickers_batch = all_tickers[i:i + BATCH_SIZE]
            logging.info(f"--- Processing batch {i//BATCH_SIZE + 1} ({len(tickers_batch)} tickers) ---")
            
            start_dates = self.get_start_dates(tickers_batch)
            batch_dfs = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_ticker = {executor.submit(self.fetch_prices_for_ticker, t, start_dates[t], datetime.date.today()): t for t in tickers_batch}
                for future in concurrent.futures.as_completed(future_to_ticker):
                    result_df = future.result()
                    if not result_df.empty: batch_dfs.append(result_df)
            
            if not batch_dfs:
                logging.info(f"Batch {i//BATCH_SIZE + 1}: No new price data to load.")
                continue
            
            final_df = pd.concat(batch_dfs, ignore_index=True)
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema=[
                bigquery.SchemaField("ticker", "STRING", "REQUIRED"), bigquery.SchemaField("date", "DATE", "REQUIRED"),
                bigquery.SchemaField("open", "FLOAT"), bigquery.SchemaField("high", "FLOAT"), bigquery.SchemaField("low", "FLOAT"),
                bigquery.SchemaField("adj_close", "FLOAT"), bigquery.SchemaField("volume", "INTEGER"),
            ])
            
            try:
                table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
                load_job = self.bq_client.load_table_from_dataframe(final_df, table_id, job_config=job_config)
                load_job.result()
                total_rows_loaded += load_job.output_rows
                logging.info(f"Batch {i//BATCH_SIZE + 1}: Successfully loaded {load_job.output_rows} rows.")
            except Exception as e:
                logging.error(f"FATAL: BQ load failed for batch {i//BATCH_SIZE + 1}: {e}")
        
        msg = f"Job complete. Total rows loaded across all batches: {total_rows_loaded}"
        logging.info(msg)
        return msg, 200

# ==============================================================================
# MAIN CLOUD FUNCTION ENTRYPOINT
# ==============================================================================
def populate_price_data(request):
    loader = PriceLoader()
    return loader.run()