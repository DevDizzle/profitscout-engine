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