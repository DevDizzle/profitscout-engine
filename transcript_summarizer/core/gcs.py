# transcript_summarizer/core/gcs.py
from google.cloud import storage

def _client() -> storage.Client:
    return storage.Client()

def read_blob(bucket: str, blob: str, encoding: str = "utf-8") -> str:
    return _client().bucket(bucket).blob(blob).download_as_text(encoding=encoding)

def write_text(bucket: str, blob: str, data: str) -> None:
    _client().bucket(bucket).blob(blob).upload_from_string(
        data, content_type="text/plain"
    )

def blob_exists(bucket: str, blob: str) -> bool:
    return _client().bucket(bucket).blob(blob).exists()

def list_blobs(bucket: str, prefix: str):
    return _client().bucket(bucket).list_blobs(prefix=prefix)
