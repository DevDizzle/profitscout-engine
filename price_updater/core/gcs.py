# price_updater/core/gcs.py
import logging
import json
from google.cloud import storage
from config import TICKER_LIST_PATH

def get_tickers(storage_client: storage.Client, bucket_name: str) -> list[str]:
    """Load ticker list from a GCS text file."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(TICKER_LIST_PATH)
        if not blob.exists():
            logging.error(f"Ticker file not found: {TICKER_LIST_PATH}")
            return []

        # Download content as text and split by lines
        content = blob.download_as_text(encoding="utf-8")
        tickers = [
            line.strip().upper() for line in content.splitlines() if line.strip()
        ]
        logging.info(f"Loaded {len(tickers)} tickers from {TICKER_LIST_PATH}.")
        return tickers
    except Exception as e:
        logging.error(f"Failed to load tickers from GCS: {e}")
        return []

def upload_json_to_gcs(storage_client: storage.Client, bucket_name: str, data: dict, blob_path: str):
    """Uploads a dictionary as a JSON object to GCS."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    logging.info(f"Successfully uploaded to gs://{bucket_name}/{blob_path}")