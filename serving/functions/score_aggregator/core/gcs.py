from google.cloud import storage
import logging
import json

def _client() -> storage.Client:
    """Initializes and returns a GCS client."""
    return storage.Client()

def list_blobs_with_content(bucket_name: str, prefix: str) -> dict:
    """Lists all blobs in a prefix and returns a dict of their name and content."""
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

def write_json_to_gcs(bucket_name: str, blob_name: str, data: list):
    """Writes a list of dictionaries to a GCS blob as a JSON file."""
    client = _client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json"
    )