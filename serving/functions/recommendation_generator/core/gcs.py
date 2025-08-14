# serving/functions/recommendation_generator/core/gcs.py
import logging
from google.cloud import storage

def _client() -> storage.Client:
    """Initializes and returns a GCS client."""
    return storage.Client()

def write_text(bucket_name: str, blob_name: str, data: str, content_type: str = "text/plain"):
    """Writes a string to a blob in GCS."""
    try:
        _client().bucket(bucket_name).blob(blob_name).upload_from_string(
            data,
            content_type=content_type
        )
    except Exception as e:
        logging.error(f"Failed to write to blob {blob_name}: {e}")
        raise

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
        logging.info(f"[{ticker}] Deleting old recommendation file: {blob.name}")
        blob.delete()