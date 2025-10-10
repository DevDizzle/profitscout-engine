# enrichment/core/gcs.py

import logging
from google.cloud import storage
from . import config

def _client() -> storage.Client:
    """Helper to create a GCS client."""
    return storage.Client(project=config.PROJECT_ID)

def write_text(bucket_name: str, blob_name: str, data: str, content_type: str = "text/plain"):
    """Writes a string to a GCS blob."""
    try:
        bucket = _client().bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data, content_type=content_type)
        logging.info(f"Successfully wrote data to gs://{bucket_name}/{blob_name}")
    except Exception as e:
        logging.error(f"Failed to write to blob {blob_name}: {e}", exc_info=True)
        raise