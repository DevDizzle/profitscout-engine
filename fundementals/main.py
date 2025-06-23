import datetime
import json
import logging
import os
import tempfile
import pandas as pd
import requests
from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_exponential
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
from google.cloud import pubsub_v1 
import time


# ---------- CONFIGURATION ----------
FMP_API_KEY     = os.getenv("FMP_API_KEY")
PROJECT_ID      = os.getenv("PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_FOLDER      = "fundamentals/"
MAX_THREADS     = 6
QUARTERS_TO_FETCH = 1          
RETENTION_MONTHS  = 24

# ---------- AUTH ----------
storage_client = storage.Client(project=PROJECT_ID)
bucket         = storage_client.bucket(GCS_BUCKET_NAME)

# ---------- RATE LIMITER ----------
class RateLimiter:
    def __init__(self, rate_limit, period):
        self.rate_limit = rate_limit
        self.period     = period
        self.requests   = []
        self.lock       = Lock()
    def acquire(self):
        with self.lock:
            now = datetime.datetime.now().timestamp()
            self.requests = [t for t in self.requests if now - t < self.period]
            if len(self.requests) >= self.rate_limit:
                time.sleep(self.period - (now - self.requests[0]))
            self.requests.append(now)
rate_limiter = RateLimiter(rate_limit=50, period=1.0)

# ---------- HELPERS ----------
def get_tickers():
    blob = bucket.blob("tickerlist.xlsx")
    if not blob.exists():
        logging.error("tickerlist.xlsx not found in GCS")
        return []
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        blob.download_to_filename(tmp.name)
        df = pd.read_excel(tmp.name)
    os.remove(tmp.name)
    return df["Ticker"].str.upper().dropna().unique().tolist()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def fetch_quarter_end_dates(ticker, limit=1):  # ONLY most recent
    rate_limiter.acquire()
    url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit={limit}&apikey={FMP_API_KEY}"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    data = response.json()
    return [item['date'] for item in data if item.get('date')]

def find_match_by_quarter(data, quarter_end):
    return next((item for item in data if item.get("date") == quarter_end), None)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def fetch_fmp_json(ticker, endpoint):
    rate_limiter.acquire()
    url = f"https://financialmodelingprep.com/api/v3/{endpoint}/{ticker}?period=quarter&limit=40&apikey={FMP_API_KEY}"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.json()

def upload_json_to_gcs(data, blob_path):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w", encoding="utf-8") as tf:
        json.dump(data, tf, indent=2)
        tf.flush()
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(tf.name)
    os.remove(tf.name)

def cleanup_old_fundamentals(ticker):
    """Archive or delete blobs older than RETENTION_MONTHS."""
    cutoff = datetime.date.today() - datetime.timedelta(days=30 * RETENTION_MONTHS)
    for endpoint in ["key-metrics", "ratios"]:
        prefix = f"{GCS_FOLDER}{ticker}/{endpoint}/"
        for blob in bucket.list_blobs(prefix=prefix):
            name = os.path.basename(blob.name)
            try:
                # expect name like TICKER_YYYY-MM-DD.json
                parts = name.split('_')
                if len(parts) < 2: continue
                date_part = parts[1].split('.')[0]
                file_date = datetime.date.fromisoformat(date_part)
                if file_date < cutoff:
                    blob.delete()
                    logging.info(f"Archived/deleted {blob.name} (older than {cutoff})")
            except Exception:
                continue

def process_fundamentals(ticker, quarters):
    try:
        for endpoint in ["key-metrics", "ratios"]:
            records = fetch_fmp_json(ticker, endpoint)
            for q in quarters:
                match = find_match_by_quarter(records, q)
                if not match:
                    logging.info(f"No {endpoint} data for {ticker} {q}")
                    continue
                blob_path = f"{GCS_FOLDER}{ticker}/{endpoint}/{ticker}_{q}.json"
                # Always upload/overwrite (Cloud Storage overwrites by default)
                upload_json_to_gcs(match, blob_path)
        cleanup_old_fundamentals(ticker)
    except Exception as e:
        logging.error(f"{ticker}: {e}")

# ─── PRODUCER: one Pub/Sub msg per ticker ──────────────────────────────────
def produce(request):
    tickers = get_tickers()
    if not tickers:
        return "No tickers", 200

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), "fmp-fundamentals-sync")

    for t in tickers:
        publisher.publish(topic_path, json.dumps({"ticker": t}).encode())

    return f"Queued {len(tickers)} tickers", 200

# ─── WORKER: one ticker per message ────────────────────────────────────────
def worker(event, context):
    try:
        payload = json.loads(base64.b64decode(event["data"]).decode())
        ticker  = payload["ticker"].upper()

        # --- grab most-recent quarter(s) then process ---
        quarters = fetch_quarter_end_dates(ticker, QUARTERS_TO_FETCH)
        if quarters:
            process_fundamentals(ticker, quarters)
        else:
            logging.info(f"No quarter-end dates for {ticker}")

        return f"✔ {ticker}"
    except Exception as e:
        logging.exception("Worker failure")
        raise e    # Pub/Sub will retry

# ---------- ENTRYPOINT ----------
def main(request, context):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")
    tickers = get_tickers()
    if not tickers:
        logging.error("No tickers to process")
        return "No tickers", 200
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = []
        for ticker in tickers:
            quarters = fetch_quarter_end_dates(ticker, QUARTERS_TO_FETCH)
            if not quarters:
                logging.info(f"No quarter-end dates for {ticker}, skipping")
                continue
            futures.append(executor.submit(process_fundamentals, ticker, quarters))
        for future in as_completed(futures):
            future.result()
    logging.info("✅ All key metrics and ratios up-to-date.")
    return "All key metrics and ratios uploaded", 200
