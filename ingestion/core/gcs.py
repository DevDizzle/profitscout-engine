# ingestion/core/gcs.py
import logging
import json
from google.cloud import storage
from . import config

def get_tickers(storage_client: storage.Client) -> list[str]:
    """Load ticker list from the GCS text file specified in config."""
    try:
        bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(config.TICKER_LIST_PATH)
        if not blob.exists():
            logging.error(f"Ticker file not found: {config.TICKER_LIST_PATH}")
            return []
        content = blob.download_as_text(encoding="utf-8")
        tickers = [line.strip().upper() for line in content.splitlines() if line.strip()]
        logging.info(f"Loaded {len(tickers)} tickers from GCS.")
        return tickers
    except Exception as e:
        logging.error(f"Failed to load tickers from GCS: {e}")
        return []

def blob_exists(storage_client: storage.Client, blob_path: str) -> bool:
    """Checks if a blob exists in GCS."""
    bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
    return storage.Blob(bucket=bucket, name=blob_path).exists(storage_client)

def upload_json_to_gcs(storage_client: storage.Client, data: dict | list, blob_path: str):
    """Uploads a dictionary or list as a JSON object to GCS."""
    try:
        bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
        logging.info(f"Successfully uploaded to gs://{config.GCS_BUCKET_NAME}/{blob_path}")
    except Exception as e:
        logging.error(f"Failed to upload JSON to {blob_path}: {e}", exc_info=True)


def cleanup_old_files(storage_client: storage.Client, folder: str, ticker: str, keep_filename: str):
    """Deletes all files for a ticker in a folder except for the one to keep."""
    bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
    prefix = f"{folder}{ticker}_"
    blobs_to_delete = [
        blob for blob in bucket.list_blobs(prefix=prefix)
        if blob.name != keep_filename
    ]
    for blob in blobs_to_delete:
        logging.info(f"Deleting old file: {blob.name}")
        blob.delete()

def list_existing_transcripts(storage_client: storage.Client) -> set:
    """Lists existing transcript files for the diff-based approach."""
    prefix = config.TRANSCRIPT_OUTPUT_FOLDER
    logging.info(f"Listing existing objects in gs://{config.GCS_BUCKET_NAME}/{prefix}")
    blobs = storage_client.list_blobs(config.GCS_BUCKET_NAME, prefix=prefix)
    existing_files = set()
    for blob in blobs:
        try:
            filename_with_ext = blob.name.split('/')[-1]
            filename_without_ext = filename_with_ext.rsplit('.', 1)[0]
            ticker, date_str = filename_without_ext.split('_', 1)
            existing_files.add((ticker, date_str))
        except (ValueError, IndexError):
            logging.warning(f"Could not parse filename: {blob.name}. Skipping.")
            continue
    logging.info(f"Found {len(existing_files)} existing transcripts in GCS.")
    return existing_files