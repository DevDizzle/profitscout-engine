# serving/core/gcs.py
import logging
import json
from datetime import date
from google.cloud import storage
from google.cloud.storage import Blob
from google.api_core import retry
from . import config

# --- Singleton Client & Retry Policy ---
_STORAGE_CLIENT = None

_RETRY_POLICY = retry.Retry(
    predicate=retry.if_exception_type(
        Exception # Retry on most errors including transient network/SSL
    ),
    initial=1.0,
    maximum=60.0,
    multiplier=2.0,
    deadline=30.0,  # Reduced to 30s to fail fast
)

def _client() -> storage.Client:
    """Returns a shared GCS client instance (Singleton)."""
    global _STORAGE_CLIENT
    if _STORAGE_CLIENT is None:
        _STORAGE_CLIENT = storage.Client()
    return _STORAGE_CLIENT

def list_blobs(bucket_name: str, prefix: str | None = None) -> list[str]:
    """Lists all the blob names in a GCS bucket with a given prefix, using robust retries."""
    try:
        client = _client()
        # Note: client.list_blobs returns an iterator. The API call happens when we iterate.
        # We wrap the iteration in a retry or rely on the client's internal retry.
        # However, list_blobs(retry=...) is supported in newer libs.
        blobs = client.list_blobs(bucket_name, prefix=prefix, retry=_RETRY_POLICY)
        return [blob.name for blob in blobs]
    except Exception as e:
        logging.error(f"Failed to list blobs in {bucket_name}/{prefix}: {e}")
        return []

def read_blob(bucket_name: str, blob_name: str, encoding: str = "utf-8") -> str | None:
    """Reads a blob from GCS and returns its content as a string."""
    try:
        bucket = _client().bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_text(encoding=encoding, retry=_RETRY_POLICY)
    except Exception as e:
        logging.error(f"Failed to read blob {blob_name}: {e}")
        return None

def write_text(bucket_name: str, blob_name: str, data: str, content_type: str = "text/plain"):
    """Writes a string to a GCS blob."""
    try:
        _client().bucket(bucket_name).blob(blob_name).upload_from_string(
            data, content_type=content_type, retry=_RETRY_POLICY
        )
    except Exception as e:
        logging.error(f"Failed to write to blob {blob_name}: {e}")
        raise

def delete_blob(bucket_name: str, blob_name: str):
    """Deletes a blob from the bucket."""
    try:
        bucket = _client().bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete(retry=_RETRY_POLICY)
        logging.info(f"Blob {blob_name} deleted.")
    except Exception as e:
        logging.error(f"Failed to delete blob {blob_name}: {e}")
        raise

def get_tickers() -> list[str]:
    """Loads the official ticker list from the GCS bucket."""
    try:
        bucket = _client().bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(config.TICKER_LIST_PATH)
        content = blob.download_as_text(encoding="utf-8", retry=_RETRY_POLICY)
        return [line.strip().upper() for line in content.splitlines() if line.strip()]
    except Exception as e:
        logging.error(f"Failed to load tickers from GCS: {e}")
        return []

def upload_from_filename(bucket_name: str, source_file_path: str, destination_blob_name: str, content_type: str = "image/png") -> str | None:
    """Uploads a local file to GCS and returns its GCS URI."""
    try:
        client = _client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path, content_type=content_type, retry=_RETRY_POLICY)
        return f"gs://{bucket_name}/{destination_blob_name}"
    except Exception as e:
        logging.error(f"Failed to upload {source_file_path} to GCS: {e}", exc_info=True)
        return None

def get_latest_blob_for_ticker(bucket_name: str, prefix: str, ticker: str) -> Blob | None:
    """Finds the most recent blob for a ticker in a given folder."""
    client = _client()
    blobs = client.list_blobs(bucket_name, prefix=f"{prefix}{ticker}_", retry=_RETRY_POLICY)
    
    latest_blob = None
    latest_date = None

    for blob in blobs:
        try:
            date_str = blob.name.split('_')[-1].split('.')[0]
            blob_date = date.fromisoformat(date_str)
            if latest_date is None or blob_date > latest_date:
                latest_date = blob_date
                latest_blob = blob
        except (ValueError, IndexError):
            continue
            
    return latest_blob