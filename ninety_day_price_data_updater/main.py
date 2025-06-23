import datetime
import logging
import os
import pandas as pd
import requests
from google.cloud import storage
from threading import Lock
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
import json
import base64
from google.cloud import pubsub_v1

# ---------- CONFIGURATION ----------
FMP_API_KEY = os.getenv("FMP_API_KEY")
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")
GCS_FOLDER = "prices/"
TICKER_LIST_PATH = "tickerlist.xlsx"
TODAY = datetime.date.today()
START_DATE = TODAY - datetime.timedelta(days=90)
END_DATE = TODAY

# ---------- AUTH ----------
storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(GCS_BUCKET_NAME)

# ---------- LOGGING SETUP ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- RATE LIMITER ----------
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
                logging.info(f"⏱ Rate limit reached. Sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
            self.calls_timestamps.append(time.time())

rate_limiter = RateLimiter(max_calls_per_period=50, period=1.0)

# ---------- HELPERS ----------
def load_tickers() -> list[str]:
    try:
        blob = bucket.blob(TICKER_LIST_PATH)
        if not blob.exists():
            logging.error("tickerlist.xlsx not found in GCS")
            return []
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
            blob.download_to_filename(temp_file.name)
            df = pd.read_excel(temp_file.name)
            temp_file.close()
            os.remove(temp_file.name)
        tickers = (
            df["Ticker"]
            .dropna()
            .astype(str)
            .str.upper()
            .loc[lambda s: s.str.match(r"^[A-Z0-9]+$")]
            .tolist()
        )
        logging.info(f"✅ Loaded {len(tickers)} tickers from GCS")
        return tickers
    except Exception as e:
        logging.error(f"Failed to load tickers: {e}")
        return []

def fetch_prices_fmp_threaded(ticker: str) -> tuple[str, list[dict] | None]:
    try:
        rate_limiter.acquire()
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?timeseries=90&apikey={FMP_API_KEY}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        hist = data.get("historical", [])
        if not hist:
            logging.info(f"{ticker}: no historical data returned")
            return (ticker, None)
        records = [
            {
                "date": entry.get("date"),
                "open": float(entry.get("open", 0.0)),
                "high": float(entry.get("high", 0.0)),
                "low": float(entry.get("low", 0.0)),
                "close": float(entry.get("close", 0.0)),
                "adjClose": float(entry.get("adjClose", 0.0)),
                "volume": int(entry.get("volume", 0)),
            }
            for entry in hist
        ]
        return (ticker, records)
    except requests.HTTPError as http_err:
        logging.warning(f"{ticker}: HTTP error → {http_err}")
        return (ticker, None)
    except Exception as e:
        logging.warning(f"{ticker}: fetch failed → {e}")
        return (ticker, None)

# ── PRODUCER: HTTP entry-point – queues one Pub/Sub message per ticker ──
def produce(request):
    tickers = load_tickers()
    if not tickers:
        return "No tickers", 200

    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), "ninety-day-prices-sync")

    for t in tickers:
        publisher.publish(topic_path, json.dumps({"ticker": t}).encode())

    return f"Queued {len(tickers)} tickers", 200

# ── WORKER: Pub/Sub entry-point – processes ONE ticker per message ──
def worker(event, context):
    try:
        payload = json.loads(base64.b64decode(event["data"]).decode())
        ticker  = payload["ticker"].upper()
        tk, records = fetch_prices_fmp_threaded(ticker)
        if not records:
            logging.info(f"{tk}: no price data to upload")
            return f"No price data for {tk}"
        json_doc = {
            "ticker": tk,
            "as_of": TODAY.isoformat(),
            "prices": records
        }
        blob_path = f"{GCS_FOLDER}{tk}/90_day_prices.json"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(json.dumps(json_doc, indent=2), content_type="application/json")
        logging.info(f"✅ Uploaded gs://{GCS_BUCKET_NAME}/{blob_path}")
        return f"Uploaded {tk}"
    except Exception as exc:
        logging.exception(f"Worker failed for {ticker if 'ticker' in locals() else 'unknown'}")
        raise

# ---------- MAIN ----------
def main(request, context=None):
    logging.info("=== Starting 90-day price update ===")
    tickers = load_tickers()
    total = len(tickers)
    if not tickers:
        logging.error("No tickers found – exiting.")
        return "No tickers found", 400

    max_workers = 3
    logging.info(f"Starting ThreadPoolExecutor with max_workers={max_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {executor.submit(fetch_prices_fmp_threaded, tk): tk for tk in tickers}
        completed = 0
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            completed += 1
            try:
                tk, records = future.result()
            except Exception as e:
                logging.warning(f"[{completed}/{total}] {ticker}: unexpected error → {e}")
                continue
            if not records:
                logging.info(f"[{completed}/{total}] {tk}: no price data to upload")
                continue
            json_doc = {
                "ticker": tk,
                "as_of": TODAY.isoformat(),
                "prices": records
            }
            blob_path = f"{GCS_FOLDER}{tk}/90_day_prices.json"
            blob = bucket.blob(blob_path)
            blob.upload_from_string(json.dumps(json_doc, indent=2), content_type="application/json")
            logging.info(f"[{completed}/{total}] ✅ Uploaded gs://{GCS_BUCKET_NAME}/{blob_path}")

    logging.info("=== 90-day price update complete ===")
    return "90-day price update completed", 200