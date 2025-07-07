# transcript_collector/core/gcs.py
import logging
import json
import os
import re
from google.cloud import storage

def list_existing_transcripts(storage_client: storage.Client, bucket_name: str, prefix: str) -> set:
    """
    Lists all blobs in a GCS prefix and parses their names to return a set of
    (ticker, date_string) tuples for efficient lookup.
    """
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    pattern = re.compile(r"([A-Z0-9\.]+)_(\d{4}-\d{2}-\d{2})\.json$")

    existing_set = set()
    for blob in blobs:
        match = pattern.search(os.path.basename(blob.name))
        if match:
            ticker, date_str = match.groups()
            existing_set.add((ticker, date_str))

    logging.info(f"Found {len(existing_set)} existing transcripts in GCS.")
    return existing_set


def upload_json_to_gcs(storage_client: storage.Client, bucket_name: str, data: dict, blob_path: str):
    """Uploads a dictionary as a JSON object to GCS."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    logging.info(f"Successfully uploaded to gs://{bucket_name}/{blob_path}")