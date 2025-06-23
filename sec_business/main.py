import os
import json
import logging
import tempfile
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
import base64
from google.cloud import pubsub_v1 
import pandas as pd
from sec_api import QueryApi, ExtractorApi
from google.cloud import storage

# ---------- CONFIG ----------
SEC_API_KEY      = os.getenv("SEC_API_KEY")
PROJECT_ID       = os.getenv("PROJECT_ID")
GCS_BUCKET_NAME  = os.getenv("GCS_BUCKET_NAME")
GCS_FOLDER       = "sec-business/"
MAX_THREADS      = 3

# ---------- AUTH ----------
storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(GCS_BUCKET_NAME)

# ---------- HELPERS ----------
def get_tickers() -> list[str]:
    """Load ticker list from GCS Excel file."""
    try:
        blob = bucket.blob("tickerlist.xlsx")
        if not blob.exists():
            logging.error("ticker-list.xlsx not found in GCS")
            return []

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            blob.download_to_filename(tmp.name)
            df = pd.read_excel(tmp.name)
        os.remove(tmp.name)

        tickers = df["Ticker"].str.upper().dropna().unique().tolist()
        logging.info("✅ Loaded %d tickers.", len(tickers))
        return tickers
    except Exception as e:
        logging.error("Failed to load tickers: %s", e)
        return []

SECTION_MAP = {
    "10-K":  "1",
    "10-KT": "1",
    "20-F":  "item4",
    "40-F":  "1",
}

def extract_business(url: str, form_type: str, extractor: ExtractorApi) -> Optional[str]:
    """Fetch the Business section text for the given filing URL."""
    key = SECTION_MAP.get(form_type)
    if not key:
        return None
    try:
        return extractor.get_section(url, key, "text")
    except Exception as e:
        if "filing type not supported" in str(e):
            logging.warning("⚠️  %s not supported for %s", form_type, url)
            return None
        raise

def process(ticker: str, query_api: QueryApi, extractor: ExtractorApi) -> None:
    """Download most-recent 10-K/20-F Business section for a single ticker."""
    try:
        query = {
            "query": {
                "query_string": {
                    "query": (
                        f'ticker:"{ticker}" AND '
                        '(formType:"10-K" OR formType:"20-F" OR formType:"40-F" OR formType:"10-KT") '
                        'AND NOT (formType:"10-K/A" OR formType:"20-F/A" OR formType:"40-F/A")'
                    )
                }
            },
            "from": "0",
            "size": "1",
            "sort": [{"filedAt": {"order": "desc"}}],
        }

        filings = query_api.get_filings(query).get("filings", [])
        if not filings:
            logging.warning("⚠️  No annual filing found for %s", ticker)
            return

        filing      = filings[0]
        filing_url  = filing["linkToFilingDetails"]
        form_type   = filing["formType"]
        text        = extract_business(filing_url, form_type, extractor)
        if not text:
            logging.warning("⚠️  %s: Business section unsupported for %s", ticker, form_type)
            return

        blob_path = f"{GCS_FOLDER}{ticker}.json"
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as tmp:
            json.dump({"form_type": form_type, "Item - Business": text}, tmp, indent=2)
            bucket.blob(blob_path).upload_from_filename(tmp.name)
        os.remove(tmp.name)
        logging.info("✅ Uploaded %s", blob_path)

    except Exception as exc:
        logging.error("❌ Error %s: %s", ticker, exc)

def clear_gcs_folder() -> None:
    """Delete any prior outputs in the target folder."""
    for blob in bucket.list_blobs(prefix=GCS_FOLDER):
        blob.delete()
    logging.info("🗑️  Cleared gs://%s/%s", GCS_BUCKET_NAME, GCS_FOLDER)

# ── PRODUCER: HTTP entry-point – queues 1 Pub/Sub message per ticker ──
def produce(request):
    tickers = get_tickers()
    if not tickers:
        return "No tickers", 200

    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), "sec-business-sync")

    for t in tickers:
        publisher.publish(topic_path, json.dumps({"ticker": t}).encode())

    return f"Queued {len(tickers)} tickers", 200

# ── WORKER: Pub/Sub entry-point – process ONE ticker per message ──
def worker(event, context):
    try:
        payload = json.loads(base64.b64decode(event["data"]).decode())
        ticker  = payload["ticker"].upper()

        query_api = QueryApi(api_key=SEC_API_KEY)
        extractor = ExtractorApi(api_key=SEC_API_KEY)
        process(ticker, query_api, extractor)

        return f"✔ Processed {ticker}"
    except Exception as exc:
        logging.exception(f"Worker failed for {ticker if 'ticker' in locals() else 'unknown'}")
        raise

# ---------- ENTRY POINT ----------
def main(request):  # Cloud Scheduler invokes this via HTTP GET
    """
    HTTP Cloud Function entry point.
    Ignores request body/params; returns simple 200 status when finished.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    clear_gcs_folder()

    query_api  = QueryApi(api_key=SEC_API_KEY)
    extractor  = ExtractorApi(api_key=SEC_API_KEY)
    tickers    = get_tickers()

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        for t in tickers:
            pool.submit(process, t, query_api, extractor)

    return ("Business sections fetch started", 200, {"Content-Type": "text/plain"})