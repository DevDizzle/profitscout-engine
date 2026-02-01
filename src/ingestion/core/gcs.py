# ingestion/core/gcs.py
"""
Shared helper functions for reading and writing blobs in GCS for all Ingestion services.
"""

import json
import logging

from google.cloud import storage

from . import config

logger = logging.getLogger(__name__)


def _client() -> storage.Client:
    """Initializes and returns a GCS client."""
    return storage.Client()


def get_tickers(storage_client: storage.Client) -> list[str]:
    """Loads the official ticker list from the GCS bucket."""
    try:
        bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(config.TICKER_LIST_PATH)
        if not blob.exists():
            logger.error(
                f"Ticker file not found in GCS: gs://{config.GCS_BUCKET_NAME}/{config.TICKER_LIST_PATH}"
            )
            return []

        content = blob.download_as_text(encoding="utf-8")
        tickers = [
            line.strip().upper() for line in content.splitlines() if line.strip()
        ]
        logger.info(f"Successfully loaded {len(tickers)} tickers from GCS.")
        return tickers
    except Exception as e:
        logger.critical(f"Failed to load tickers from GCS: {e}", exc_info=True)
        return []


def blob_exists(storage_client: storage.Client, blob_name: str) -> bool:
    """Checks if a blob exists in GCS."""
    try:
        bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        logger.error(f"Failed to check existence for blob {blob_name}: {e}")
        return False


def upload_json_to_gcs(storage_client: storage.Client, data: dict, blob_path: str):
    """Uploads a dictionary as a JSON file to GCS."""
    bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")


def cleanup_old_files(
    storage_client: storage.Client, folder: str, ticker: str, keep_filename: str
) -> None:
    """Deletes all files for a ticker in a folder except for the one to keep."""
    bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
    prefix = f"{folder}{ticker}_"

    blobs_to_delete = [
        blob for blob in bucket.list_blobs(prefix=prefix) if blob.name != keep_filename
    ]

    for blob in blobs_to_delete:
        logger.info(f"[{ticker}] Deleting old file: {blob.name}")
        try:
            blob.delete()
        except Exception as e:
            logger.error(f"Failed to delete blob {blob.name}: {e}")


def read_blob(bucket_name: str, blob_name: str) -> str | None:
    """Reads a blob from GCS and returns its content as a string."""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            return None
        return blob.download_as_text()
    except Exception as e:
        logger.error(f"Failed to read blob {blob_name}: {e}")
        return None


def list_existing_transcripts(storage_client: storage.Client) -> set:
    """Lists existing transcripts in GCS and returns a set of (ticker, date_str) tuples."""
    bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=config.TRANSCRIPT_OUTPUT_FOLDER)
    existing_set = set()
    for blob in blobs:
        try:
            # Assumes filename format is TICKER_YYYY-MM-DD.json
            file_name = blob.name.split("/")[-1]
            ticker, date_str = file_name.replace(".json", "").split("_")
            existing_set.add((ticker, date_str))
        except Exception:
            continue
    return existing_set
