"""Helper functions for reading and writing text blobs in GCS."""
from google.cloud import storage

def _client() -> storage.Client:
    return storage.Client()

def read_blob(bucket: str, blob: str, encoding: str = "utf-8") -> str:
    return _client().bucket(bucket).blob(blob).download_as_text(encoding=encoding)

def write_text(bucket: str, blob: str, data: str) -> None:
    _client().bucket(bucket).blob(blob).upload_from_string(data, content_type="text/plain")

def blob_exists(bucket: str, blob: str) -> bool:
    return _client().bucket(bucket).blob(blob).exists()

def list_blobs(bucket_name, prefix=None):
    """Lists all the blob names in a GCS bucket with a given prefix."""
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs]
