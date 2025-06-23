import datetime, logging, os, re, tempfile, time, pandas as pd, requests, json
from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- CONFIGURATION ----------
FMP_API_KEY        = os.getenv("FMP_API_KEY")
PROJECT_ID         = os.getenv("PROJECT_ID")
BUCKET_NAME        = os.getenv("GCS_BUCKET_NAME")
GCS_FOLDER         = "earnings-call/"
GCS_COLD_FOLDER    = "earnings-call-cold/"          # older data moves here
RETENTION_MONTHS   = 24                             # keep ≈ 8 quarters
MAX_THREADS        = 6                              # tweak as needed

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
    def __init__(self, limit, period=1.0):
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
def _rate_limited(e):   # FMP also returns 429 on burst
    return isinstance(e, requests.HTTPError) and e.response.status_code == 429

retry_429 = dict(
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=2, max=10),
    retry=retry_if_exception(_rate_limited),
    reraise=True,
)

# ---------- HELPERS ----------
@retry(**retry_429)
def safe_get(url: str):
    rate_limiter.acquire()
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()

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

def latest_quarter_date(ticker: str) -> str:
    url = (f"https://financialmodelingprep.com/api/v3/income-statement/"
           f"{ticker}?period=quarter&limit=1&apikey={FMP_API_KEY}")
    data = safe_get(url)
    if not data:
        return ""
    return data[0].get("date", "")

def year_quarter(ticker: str, end: str) -> tuple[int, int]:
    d = pd.to_datetime(end)
    if ticker == "AAPL":  # Apple’s fiscal twist
        if d.month >= 10:
            return d.year + 1, 1
        if d.month <= 3:
            return d.year, 2
        if d.month <= 6:
            return d.year, 3
        return d.year, 4
    return d.year, (d.month - 1) // 3 + 1

def blob_paths(ticker: str, end: str):
    base = f"{ticker}_{end}"
    return (f"{GCS_FOLDER}json/{base}.json", f"{GCS_FOLDER}txt/{base}.txt")

def exists(path: str) -> bool:
    return bucket.blob(path).exists()

def purge_old(ticker: str):
    cutoff = datetime.date.today() - datetime.timedelta(days=30 * RETENTION_MONTHS)
    pattern = re.compile(rf"{ticker}_(\d{{4}}-\d{{2}}-\d{{2}})\.json$")
    for blob in bucket.list_blobs(prefix=f"{GCS_FOLDER}json/{ticker}_"):
        m = pattern.search(blob.name)
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

@retry(**retry_429)
def fetch_transcript(ticker: str, year: int, quarter: int) -> dict:
    url = (f"https://financialmodelingprep.com/api/v3/earning_call_transcript/"
           f"{ticker}?year={year}&quarter={quarter}&apikey={FMP_API_KEY}")
    data = safe_get(url)
    return data[0] if data else {}

def process(ticker: str, end: str):
    year, qtr = year_quarter(ticker, end)
    json_blob, _ = blob_paths(ticker, end)

    if exists(json_blob):
        logger.info(f"{ticker}: latest transcript ({end}) already exists, skipping")
        return

    transcript = fetch_transcript(ticker, year, qtr)
    if not (transcript and transcript.get("content")):
        logger.warning(f"{ticker}: no transcript content for {year} Q{qtr}")
        return

    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json", encoding="utf-8") as jf:
        json.dump(transcript, jf)
        json_path = jf.name

    bucket.blob(json_blob).upload_from_filename(json_path)
    os.remove(json_path)

# ---------- MAIN (HTTP) ----------
def main(request, context=None):             
    tickers = list_tickers()
    if not tickers:
        logger.info("No tickers to process; exiting.")
        return "No tickers", 200

    logger.info(f"Starting earnings-call sync for {len(tickers)} tickers")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        futures = []
        for t in tickers:
            end = latest_quarter_date(t)
            if not end:
                logger.warning(f"{t}: could not fetch latest quarter date, skipping")
                continue
            logger.info(f"{t}: latest quarter is {end}")
            purge_old(t)                     # clean any >24 months old
            futures.append(pool.submit(process, t, end))

        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                logger.error(f"Error during processing: {e}")

    logger.info("Earnings-call JSON sync complete")
    return "Sync complete", 200
