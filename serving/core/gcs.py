# serving/core/gcs.py
import logging
import json
from google.cloud import storage
from . import config

def _client() -> storage.Client:
    return storage.Client()

def list_blobs_with_content(bucket_name: str, prefix: str) -> dict:
    client = _client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    content_map = {}
    for blob in blobs:
        try:
            content = blob.download_as_text()
            content_map[blob.name] = content
        except Exception as e:
            logging.error(f"Failed to read blob {blob.name}: {e}")
    return content_map

def list_blobs(bucket_name: str, prefix: str | None = None) -> list[str]:
    """Lists all the blob names in a GCS bucket with a given prefix."""
    blobs = _client().list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs]

def read_blob(bucket_name: str, blob_name: str, encoding: str = "utf-8") -> str | None:
    """Reads a blob from GCS and returns its content as a string."""
    try:
        bucket = _client().bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_text(encoding=encoding)
    except Exception as e:
        logging.error(f"Failed to read blob {blob_name}: {e}")
        return None

def write_text(bucket_name: str, blob_name: str, data: str, content_type: str = "text/plain"):
    try:
        _client().bucket(bucket_name).blob(blob_name).upload_from_string(data, content_type)
    except Exception as e:
        logging.error(f"Failed to write to blob {blob_name}: {e}")
        raise

def delete_blob(bucket_name: str, blob_name: str):
    """Deletes a blob from GCS."""
    try:
        _client().bucket(bucket_name).blob(blob_name).delete()
        logging.info(f"Successfully deleted {blob_name}")
    except Exception as e:
        logging.error(f"Failed to delete blob {blob_name}: {e}")
        raise

def cleanup_old_files(bucket_name: str, folder: str, ticker: str, keep_filename: str):
    bucket = _client().bucket(bucket_name)
    prefix = f"{folder}{ticker}_"
    blobs_to_delete = [b for b in bucket.list_blobs(prefix=prefix) if b.name != keep_filename]
    for blob in blobs_to_delete:
        logging.info(f"[{ticker}] Deleting old file: {blob.name}")
        blob.delete()

def get_tickers() -> list[str]:
    try:
        bucket = _client().bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(config.TICKER_LIST_PATH)
        content = blob.download_as_text(encoding="utf-8")
        return [line.strip().upper() for line in content.splitlines() if line.strip()]
    except Exception as e:
        logging.error(f"Failed to load tickers from GCS: {e}")
        return []

def delete_folder_contents(bucket_name: str, prefix: str):
    """Deletes all blobs within a specified folder (prefix) in a GCS bucket."""
    client = _client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    
    deleted_count = 0
    for blob in blobs:
        blob.delete()
        deleted_count += 1
    
    if deleted_count > 0:
        logging.info(f"Deleted {deleted_count} old files from gs://{bucket_name}/{prefix}")
    else:
        logging.info(f"No old files found in gs://{bucket_name}/{prefix} to delete.")