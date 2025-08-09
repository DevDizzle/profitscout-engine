# enrichment/functions/transcript_summarizer/core/gcs.py

"""Helper functions for reading and writing text blobs in GCS."""
from google.cloud import storage
import logging

def _client() -> storage.Client:
    """Initializes and returns a GCS client."""
    return storage.Client()

def read_blob(bucket: str, blob: str, encoding: str = "utf-8") -> str:
    """Reads a blob from GCS and returns its content as a string."""
    return _client().bucket(bucket).blob(blob).download_as_text(encoding=encoding)

def write_text(bucket: str, blob: str, data: str) -> None:
    """Writes a string to a blob in GCS."""
    _client().bucket(bucket).blob(blob).upload_from_string(data, content_type="text/plain")

def blob_exists(bucket: str, blob: str) -> bool:
    """Checks if a blob exists in GCS."""
    return _client().bucket(bucket).blob(blob).exists()

def list_blobs(bucket_name, prefix=None):
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
        logging.info(f"Deleting old file: {blob.name}")
        blob.delete()