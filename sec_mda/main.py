import os, re, json, logging, tempfile, datetime, time, pandas as pd
from typing import Optional
from dateutil.relativedelta import relativedelta
from sec_api import QueryApi, ExtractorApi
from google.cloud import storage
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception
import base64
from google.cloud import pubsub_v1

# ---------- CONFIG ----------
SEC_API_KEY      = os.getenv("SEC_API_KEY")
PROJECT_ID       = os.getenv("PROJECT_ID")
BUCKET_NAME      = os.getenv("GCS_BUCKET_NAME")
GCS_FOLDER       = "sec-mda/"
COLD_FOLDER      = "sec-mda-cold/"
RETENTION_MONTHS = 24              # keep ≈ 8 quarters
PAUSE_SECONDS    = 0.25            # polite delay between tickers

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------- GCS ----------
storage_client = storage.Client(project=PROJECT_ID)
bucket         = storage_client.bucket(BUCKET_NAME)
BLOB_RE        = re.compile(r"^([^_]+)_(\d{4}-\d{2}-\d{2})_(10-[KQ]T?)\.json$")

# ---------- SEC-API HELPERS ----------
def _rate_limited(e):
    return "429" in str(e) or "Too many requests" in str(e)

retry_429 = dict(
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception(_rate_limited),
)

@retry(**retry_429)
def safe_filings(api: QueryApi, query: dict) -> dict:
    return api.get_filings(query)

@retry(**retry_429)
def safe_extract(extractor: ExtractorApi, url: str, key: str) -> str:
    return extractor.get_section(url, key, "text")

# ---------- SECTION MAP ----------
SECTION_MAP = {"10-K": "7", "10-KT": "7", "10-Q": "part1item2", "10-QT": "part1item2"}

# ---------- HELPERS ----------
def get_tickers() -> list[str]:
    blob = bucket.blob("tickerlist.xlsx")
    if not blob.exists():
        logger.error("tickerlist.xlsx not found in GCS")
        return []
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        blob.download_to_filename(tmp.name)
        df = pd.read_excel(tmp.name)
    os.remove(tmp.name)
    return df["Ticker"].str.upper().dropna().unique().tolist()

def purge_old_files(ticker: str):
    cutoff = datetime.date.today() - relativedelta(months=RETENTION_MONTHS)
    prefix = f"{GCS_FOLDER}{ticker}_"
    for blob in bucket.list_blobs(prefix=prefix):
        m = BLOB_RE.match(blob.name.replace(GCS_FOLDER, ""))
        if not m:
            continue
        _, date_str, _ = m.groups()
        try:
            if datetime.date.fromisoformat(date_str) < cutoff:
                cold_path = blob.name.replace(GCS_FOLDER, COLD_FOLDER)
                bucket.copy_blob(blob, bucket, cold_path)
                blob.delete()
                logger.info(f"Archived {blob.name} to {cold_path}")
        except ValueError:
            continue

def process_ticker(ticker: str, query_api: QueryApi, extractor_api: ExtractorApi):
    # 1) Query newest 10-K or 10-Q by report period
    query = {
        "query": {"query_string": {
            "query": (f'ticker:"{ticker}" AND (formType:"10-K" OR formType:"10-Q") '
                      'AND NOT (formType:"10-K/A" OR formType:"10-Q/A")')
        }},
        "from": "0", "size": "1",
        "sort": [{"periodOfReport": {"order": "desc"}}],
    }
    filings = safe_filings(query_api, query).get("filings", [])
    if not filings:
        return

    filing   = filings[0]
    date_iso = filing["periodOfReport"][:10]
    form     = filing["formType"]
    key      = SECTION_MAP.get(form)
    if not key:
        return

    # Skip filings older than retention window
    period = datetime.date.fromisoformat(date_iso)
    if period < datetime.date.today() - relativedelta(months=RETENTION_MONTHS):
        logger.info(f"Skipping {ticker} filing from {period} (older than retention window)")
        return

    blob_path = f"{GCS_FOLDER}{ticker}_{date_iso}_{form}.json"
    if bucket.blob(blob_path).exists():
        purge_old_files(ticker)
        return

    # 2) Extract MD&A
    text = safe_extract(extractor_api, filing["linkToFilingDetails"], key)
    if not text:
        return

    # 3) Upload
    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as tmp:
        json.dump({"MD&A": text}, tmp, indent=2)
        bucket.blob(blob_path).upload_from_filename(tmp.name)
    os.remove(tmp.name)
    logger.info(f"Uploaded {blob_path}")

    # 4) Archive old files
    purge_old_files(ticker)

# ── PRODUCER: HTTP entry-point – queues one Pub/Sub message per ticker ──
def produce(request):
    tickers = get_tickers()
    if not tickers:
        return "No tickers", 200

    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), "sec-mda-sync")

    for t in tickers:
        publisher.publish(topic_path, json.dumps({"ticker": t}).encode())

    return f"Queued {len(tickers)} tickers", 200

# ── WORKER: Pub/Sub entry-point – processes ONE ticker per message ──
def worker(event, context):
    try:
        payload = json.loads(base64.b64decode(event["data"]).decode())
        ticker  = payload["ticker"].upper()
        query_api     = QueryApi(api_key=SEC_API_KEY)
        extractor_api = ExtractorApi(api_key=SEC_API_KEY)
        process_ticker(ticker, query_api, extractor_api)
        return f"✔ Processed {ticker}"
    except Exception as exc:
        logger.exception(f"Worker failed for {ticker if 'ticker' in locals() else 'unknown'}")
        raise

# ---------- MAIN (HTTP) ----------
def main(request):
    query_api     = QueryApi(api_key=SEC_API_KEY)
    extractor_api = ExtractorApi(api_key=SEC_API_KEY)
    for ticker in get_tickers():
        try:
            process_ticker(ticker, query_api, extractor_api)
        except Exception as e:
            logger.error(f"{ticker}: {e}")
        time.sleep(PAUSE_SECONDS)  # polite delay

    return "MD&A incremental fetch complete", 200
