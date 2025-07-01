# transcript_summarizer/core/gcs.py
"""
Very small helper for reading/writing text blobs in GCS.
"""
from google.cloud import storage

def _client() -> storage.Client:           # local helper
    return storage.Client()

def write_text(bucket: str, blob: str, data: str) -> None:
    bkt  = _client().bucket(bucket)
    bkt.blob(blob).upload_from_string(data, content_type="text/plain")

def read_text(bucket: str, blob: str) -> str:
    bkt  = _client().bucket(bucket)
    return bkt.blob(blob).download_as_text()