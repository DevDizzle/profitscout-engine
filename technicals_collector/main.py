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

# ---------- CONFIGURATION ----------
FMP_API_KEY    = os.getenv("FMP_API_KEY")
PROJECT_ID     = os.getenv("PROJECT_ID")
GCS_BUCKET_NAME= os.getenv("GCS_BUCKET_NAME")
GCS_FOLDER     = "technicals/"
MAX_THREADS    = 6
DAYS_TO_KEEP   = 90

# ---------- AUTH ----------
storage_client = storage.Client(project=PROJECT_ID)
bucket         = storage_client.bucket(GCS_BUCKET_NAME)

# ---------- RATE LIMITER ----------
class RateLimiter:
    def __init__(self, rate_limit: int, period: float):
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

rate_limiter = RateLimiter(50, 1.0)

# ---------- HELPERS ----------
def get_tickers() -> list[str]:
    blob = bucket.blob("tickerlist.xlsx")
    if not blob.exists():
        logging.error("tickerlist.xlsx not found in GCS")
        return []
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        blob.download_to_filename(tmp.name)
        df = pd.read_excel(tmp.name)
    os.remove(tmp.name)
    return df["Ticker"].str.upper().dropna().unique().tolist()

def upload_to_gcs(local_path: str, blob_path: str):
    bucket.blob(blob_path).upload_from_filename(local_path)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def fetch_indicator(ticker: str, ind: str, period: int | None) -> list[dict]:
    rate_limiter.acquire()
    url = f"https://financialmodelingprep.com/api/v3/technical_indicator/daily/{ticker}?type={ind}"
    if period:
        url += f"&period={period}"
    url += f"&apikey={FMP_API_KEY}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data[:DAYS_TO_KEEP] if isinstance(data, list) else []

INDICATORS = {
    "sma_20": {"type": "sma", "period": 20},
    "ema_50": {"type": "ema", "period": 50},
    "rsi_14": {"type": "rsi", "period": 14},
    "adx_14": {"type": "adx", "period": 14},
    "obv":    {"type": "obv", "period": None},
    "macd":   {"type": "macd", "period": None},
    "stochastic": {"type": "stochastic", "period": None},
}

# ---------- MAIN PROCESSING ----------
def process_ticker(ticker: str):
    try:
        blob_path = f"{GCS_FOLDER}{ticker}/technicals.json"

        output = {"ticker": ticker,
                  "as_of": str(datetime.date.today()),
                  "technicals": {}}

        for label, cfg in INDICATORS.items():
            data = fetch_indicator(ticker, cfg["type"], cfg["period"])
            if not data:
                continue
            if cfg["type"] == "macd":
                output["technicals"]["macd"] = [
                    {"date": d["date"], "line": d.get("macd"),
                     "signal": d.get("macdSignal"),
                     "histogram": d.get("macdHist")} for d in data]
            elif cfg["type"] == "stochastic":
                output["technicals"]["stochastic"] = [
                    {"date": d["date"], "k": d.get("stochK"),
                     "d": d.get("stochD")} for d in data]
            else:
                output["technicals"][label] = [
                    {"date": d["date"], "value": d.get(cfg["type"])} for d in data]

        # --- REMOVED local folder creation ---
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as tf:
            json.dump(output, tf, indent=2)
            local = tf.name
        upload_to_gcs(local, blob_path)         # overwrite if exists
        os.remove(local)

    except Exception as e:
        logging.error(f"{ticker}: {e}")

# ---------- ENTRYPOINT ----------
def main(request):
    tickers = get_tickers()
    if not tickers:
        return "No tickers found", 200

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        futures = [pool.submit(process_ticker, t) for t in tickers]
        for f in as_completed(futures):
            f.result()

    return "Technicals refreshed", 200
