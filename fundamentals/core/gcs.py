# fundamentals/core/gcs.py
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

# ... (the other functions in this file remain the same) ...

def blob_exists(storage_client: storage.Client, bucket_name: str, blob_path: str) -> bool:
    """Checks if a blob exists in GCS."""
    bucket = storage_client.bucket(bucket_name)
    return storage.Blob(bucket=bucket, name=blob_path).exists(storage_client)

def upload_json_to_gcs(storage_client: storage.Client, bucket_name: str, data: list, blob_path: str):
    """Uploads a list of dictionaries as a JSON object to GCS."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    logging.info(f"Successfully uploaded to gs://{bucket_name}/{blob_path}")

def cleanup_old_files(storage_client: storage.Client, bucket_name: str, folder: str, ticker: str, keep_filename: str):
    """Deletes all files for a ticker in a folder except for the one to keep."""
    bucket = storage_client.bucket(bucket_name)
    prefix = f"{folder}{ticker}_"
    blobs_to_delete = [
        blob for blob in bucket.list_blobs(prefix=prefix)
        if blob.name != keep_filename
    ]
    for blob in blobs_to_delete:
        logging.info(f"Deleting old file: {blob.name}")
        blob.delete()