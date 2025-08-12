# enrichment/core/gcs.py
"""
Shared helper functions for reading and writing blobs in GCS for all Enrichment services.
"""
from google.cloud import storage
import logging

def _client() -> storage.Client:
    """Initializes and returns a GCS client."""
    return storage.Client()

def blob_exists(bucket_name: str, blob_name: str) -> bool:
    """Checks if a blob exists in GCS."""
    try:
        bucket = _client().bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        logging.error(f"Failed to check existence for blob {blob_name}: {e}")
        return False

def read_blob(bucket_name: str, blob_name: str, encoding: str = "utf-8") -> str | None:
    """Reads a blob from GCS and returns its content as a string."""
    try:
        bucket = _client().bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_text(encoding=encoding)
    except Exception as e:
        logging.error(f"Failed to read blob {blob_name}: {e}")
        return None

def write_text(bucket_name: str, blob_name: str, data: str, content_type: str = "text/plain") -> None:
    """Writes a string to a blob in GCS."""
    _client().bucket(bucket_name).blob(blob_name).upload_from_string(data, content_type=content_type)

def list_blobs(bucket_name: str, prefix: str | None = None) -> list[str]:
    """Lists all the blob names in a GCS bucket with a given prefix."""
    blobs = _client().list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs]

def cleanup_old_files(bucket_name: str, folder: str, ticker: str, keep_filename: str):
    """Deletes all files for a ticker in a folder except for the one to keep."""
    client = _client()
    bucket = client.bucket(bucket_name)
    prefix = f"{folder}{ticker}_"
    
    blobs_to_delete = [
        blob for blob in bucket.list_blobs(prefix=prefix)
        if blob.name != keep_filename
    ]
    
    for blob in blobs_to_delete:
        logging.info(f"[{ticker}] Deleting old file: {blob.name}")
        blob.delete()