# enrichment/functions/transcript_summarizer/core/gcs.py

"""Helper functions for reading and writing text blobs in GCS."""
from google.cloud import storage

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
    # --- THIS IS THE FIX ---
    # It now calls the _client() function to get an initialized client.
    blobs = _client().list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs]