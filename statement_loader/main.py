import datetime, logging, os, re, tempfile, time, pandas as pd, requests, json
from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
from google.cloud import pubsub_v1 

# ---------- CONFIGURATION ----------
FMP_API_KEY        = os.getenv("FMP_API_KEY")
PROJECT_ID         = os.getenv("PROJECT_ID")
BUCKET_NAME        = os.getenv("GCS_BUCKET_NAME")
GCS_FOLDER         = "financial-statements/"
GCS_COLD_FOLDER    = "financial-statements-cold/"   # older data moves here
RETENTION_MONTHS   = 24                             # keep ≈ 8 quarters
MAX_THREADS        = 3

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------- AUTH ----------
storage_client = storage.Client(project=PROJECT_ID)
bucket         = storage_client.bucket(BUCKET_NAME)

# ---------- RATE LIMITER (50 req/s default) ----------
class RateLimiter:
    def __init__(self, limit: int, period: float = 1.0):
        self.limit, self.period = limit, period
        self.lock = Lock()
        self.timestamps = []

    def acquire(self):
        with self.lock:
            now = time.time()
            self.timestamps = [t for t in self.timestamps if now - t < self.period]
            if len(self.timestamps) >= self.limit:
                sleep = self.period - (now - self.timestamps[0])
                logger.debug(f"Rate-limit sleep {sleep:.2f}s")
                time.sleep(sleep)
            self.timestamps.append(now)

rate_limiter = RateLimiter(limit=50)

# ---------- TENACITY HELPERS ----------
def _rate_limited(e):
    return isinstance(e, requests.HTTPError) and e.response.status_code == 429

retry_429 = dict(
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=2, max=10),
    retry=retry_if_exception(_rate_limited),
    reraise=True,
)

@retry(**retry_429)
def safe_get(url: str):
    rate_limiter.acquire()
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()

# ---------- HELPERS ----------
def list_tickers() -> list[str]:
    blob = bucket.blob("tickerlist.xlsx")
    if not blob.exists():
        logger.error("tickerlist.xlsx not found in GCS")
        return []
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        blob.download_to_filename(tmp.name)
        df = pd.read_excel(tmp.name)
    os.remove(tmp.name)
    return df["Ticker"].str.upper().dropna().unique().tolist()

@retry(**retry_429)
def latest_quarter_end(ticker: str) -> str:
    url = (
        f"https://financialmodelingprep.com/api/v3/income-statement/"
        f"{ticker}?period=quarter&limit=1&apikey={FMP_API_KEY}"
    )
    data = safe_get(url)
    if not data or not data[0].get("date"):
        return ""
    return data[0]["date"]

def year_quarter(end: str) -> tuple[int, int]:
    d = pd.to_datetime(end)
    return d.year, (d.month - 1) // 3 + 1

def find_closest_statement(data: list[dict], target_date_str: str) -> dict:
    target_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
    closest = {}
    min_diff = datetime.timedelta(days=46)
    for record in data:
        rec_date_str = record.get("date")
        if not rec_date_str:
            continue
        try:
            rec_date = datetime.datetime.strptime(rec_date_str, "%Y-%m-%d").date()
            diff = abs(rec_date - target_date)
            if diff < min_diff:
                closest = record
                min_diff = diff
        except Exception:
            continue
    return closest

@retry(**retry_429)
def fetch_fmp_statement(ticker: str, statement_type: str) -> list[dict]:
    rate_limiter.acquire()
    url = (
        f"https://financialmodelingprep.com/api/v3/{statement_type}/{ticker}"
        f"?period=quarter&limit=40&apikey={FMP_API_KEY}"
    )
    data = safe_get(url)
    if not isinstance(data, list):
        logger.warning(f"⚠️ Unexpected API response for {ticker} - {statement_type}")
        return []
    return data

def blob_path_for(ticker: str, end: str) -> str:
    return f"{GCS_FOLDER}{ticker}/{ticker}_{end}.json"

def exists_in_gcs(path: str) -> bool:
    return bucket.blob(path).exists()

def upload_to_gcs(local: str, remote: str):
    bucket.blob(remote).upload_from_filename(local)
    logger.info(f"Uploaded {remote}")

def purge_old(ticker: str):
    cutoff = datetime.date.today() - datetime.timedelta(days=30 * RETENTION_MONTHS)
    prefix = f"{GCS_FOLDER}{ticker}/"
    pattern = re.compile(rf"{ticker}_(\d{{4}}-\d{{2}}-\d{{2}})\.json$")
    for blob in bucket.list_blobs(prefix=prefix):
        name = blob.name.split("/")[-1]
        m = pattern.match(name)
        if not m:
            continue
        try:
            file_date = datetime.date.fromisoformat(m.group(1))
            if file_date < cutoff:
                target = blob.name.replace(GCS_FOLDER, GCS_COLD_FOLDER)
                bucket.copy_blob(blob, bucket, target)
                blob.delete()
        except ValueError:
            continue

def process_ticker(ticker: str):
    end = latest_quarter_end(ticker)
    if not end:
        logger.warning(f"No recent quarter for {ticker}; skipping")
        return

    quota_path = blob_path_for(ticker, end)
    if exists_in_gcs(quota_path):
        logger.info(f"{ticker} latest ({end}) already exists; skipping")
        purge_old(ticker)
        return

    # Fetch all three statements
    statements = {
        "income": fetch_fmp_statement(ticker, "income-statement"),
        "balance": fetch_fmp_statement(ticker, "balance-sheet-statement"),
        "cashflow": fetch_fmp_statement(ticker, "cash-flow-statement")
    }

    income_rec  = find_closest_statement(statements["income"], end)
    balance_rec = find_closest_statement(statements["balance"], end)
    cashflow_rec= find_closest_statement(statements["cashflow"], end)

    if not any([income_rec, balance_rec, cashflow_rec]):
        logger.warning(f"No data for {ticker} at {end}; skipping")
        purge_old(ticker)
        return

    merged = {
        "symbol": ticker,
        "target_quarter": end,
        "incomeStatement": income_rec,
        "balanceSheet": balance_rec,
        "cashFlow": cashflow_rec
    }

    try:
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json", encoding="utf-8") as tf:
            json.dump(merged, tf, indent=2)
            tf.flush()
            upload_to_gcs(tf.name, quota_path)
        os.remove(tf.name)
    except Exception as e:
        logger.error(f"Error saving {ticker} {end}: {e}")

    purge_old(ticker)

# ─── PRODUCER: publish one message per ticker ──────────────────────────────
def produce(request):
    """HTTP entry-point.  Queues one Pub/Sub msg per ticker and returns fast."""
    tickers = list_tickers()
    if not tickers:
        return "No tickers found", 200

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), "fmp-sync")

    for t in tickers:
        publisher.publish(topic_path, json.dumps({"ticker": t}).encode())

    return f"Queued {len(tickers)} tickers", 200

# ─── WORKER: handle ONE ticker per message ─────────────────────────────────
def worker(event, context):
    """Pub/Sub entry-point.  Expects {"ticker": "AAPL"} payload."""
    try:
        payload = json.loads(base64.b64decode(event["data"]).decode())
        ticker  = payload["ticker"].upper()
        process_ticker(ticker)
        return f"✔ processed {ticker}"
    except Exception as e:
        logger.exception("Worker failure")
        raise e           # lets Pub/Sub redeliver on failure

# ---------- MAIN (HTTP) ----------
def main(request):
    tickers = list_tickers()
    if not tickers:
        logger.info("No tickers found; exiting.")
        return "No tickers to process", 200

    logger.info(f"Processing {len(tickers)} tickers")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        futures = [pool.submit(process_ticker, t) for t in tickers]
        for f in as_completed(futures):
            f.result()  # raise exceptions if any

    logger.info("All financial statements synced.")
    return "Financial statements incremental fetch complete", 200
