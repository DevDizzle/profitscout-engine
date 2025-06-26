import logging
import json
from google.cloud import storage
from config import TICKER_LIST_PATH, GCS_INPUT_PREFIX

def get_tickers(bucket: storage.Bucket) -> set:
    """Load ticker list from a GCS text file into a set for fast lookups."""
    try:
        blob = bucket.blob(TICKER_LIST_PATH)
        if not blob.exists():
            logging.error(f"Ticker file not found: {TICKER_LIST_PATH}")
            return set()
        content = blob.download_as_text(encoding="utf-8")
        tickers = {line.strip().upper() for line in content.splitlines() if line.strip()}
        logging.info(f"Loaded {len(tickers)} tickers to process from {TICKER_LIST_PATH}.")
        return tickers
    except Exception as e:
        logging.error(f"Failed to load tickers from GCS: {e}", exc_info=True)
        return set()

def list_blobs_for_tickers(storage_client: storage.Client, bucket: storage.Bucket) -> list[storage.Blob]:
    """Lists all blobs in a folder that correspond to a list of tickers."""
    return list(bucket.list_blobs(prefix=GCS_INPUT_PREFIX))

def blob_exists(bucket: storage.Bucket, blob_path: str) -> bool:
    """Checks if a blob exists in GCS."""
    return bucket.blob(blob_path).exists()

def download_transcript_text(blob: storage.Blob) -> str:
    """Downloads a GCS blob, parses it as JSON, and returns the transcript content."""
    try:
        data = json.loads(blob.download_as_text(encoding="utf-8"))
        for key in ("content", "transcript", "text", "body"):
            if isinstance(data, dict) and key in data and data[key]:
                return data[key]
        return json.dumps(data, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Failed to download or parse blob {blob.name}: {e}")
        return ""