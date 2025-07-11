import logging
import json
from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_exponential

def list_existing_transcripts(storage_client: storage.Client, bucket_name: str, prefix: str) -> set:
    """
    Lists all transcript files in a GCS bucket that match a given prefix.
    The filenames are expected to be in the format: {TICKER}_{YYYY-MM-DD}.json
    """
    logging.info(f"Listing existing objects in gs://{bucket_name}/{prefix}")
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    
    existing_files = set()
    for blob in blobs:
        try:
            # Extracts 'TICKER_YYYY-MM-DD' from 'earnings-call-transcripts/TICKER_YYYY-MM-DD.json'
            filename_with_ext = blob.name.split('/')[-1]
            filename_without_ext = filename_with_ext.rsplit('.', 1)[0]
            ticker, date_str = filename_without_ext.split('_', 1)
            existing_files.add((ticker, date_str))
        except (ValueError, IndexError):
            logging.warning(f"Could not parse filename: {blob.name}. Skipping.")
            continue
            
    logging.info(f"Found {len(existing_files)} existing transcripts in GCS.")
    return existing_files

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def upload_json_to_gcs(storage_client: storage.Client, bucket_name: str, data: dict, blob_path: str):
    """Uploads a dictionary as a JSON file to GCS with retry logic."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    logging.info(f"Successfully uploaded to gs://{bucket_name}/{blob_path}")