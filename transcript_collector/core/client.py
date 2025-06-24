# transcript_collector/core/gcs.py
import logging
import json
import re
import datetime
from dateutil.relativedelta import relativedelta
from google.cloud import storage
from config import TICKER_LIST_PATH, RETENTION_MONTHS

def get_tickers(storage_client: storage.Client, bucket_name: str) -> list[str]:
    """Load ticker list from a GCS text file."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(TICKER_LIST_PATH)
        if not blob.exists():
            logging.error(f"Ticker file not found: {TICKER_LIST_PATH}")
            return []

        # Download content as text and split by lines
        content = blob.download_as_text(encoding="utf-8")
        tickers = [
            line.strip().upper() for line in content.splitlines() if line.strip()
        ]
        logging.info(f"Loaded {len(tickers)} tickers from {TICKER_LIST_PATH}.")
        return tickers
    except Exception as e:
        logging.error(f"Failed to load tickers from GCS: {e}")
        return []

def blob_exists(storage_client: storage.Client, bucket_name: str, blob_path: str) -> bool:
    """Checks if a blob exists in GCS."""
    bucket = storage_client.bucket(bucket_name)
    return storage.Blob(bucket=bucket, name=blob_path).exists(storage_client)

def upload_json_to_gcs(storage_client: storage.Client, bucket_name: str, data: dict, blob_path: str):
    """Uploads a dictionary as a JSON object to GCS."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    logging.info(f"Successfully uploaded to gs://{bucket_name}/{blob_path}")

def archive_old_files(storage_client: storage.Client, bucket_name: str, hot_folder: str, cold_folder: str, ticker: str):
    """Moves transcripts older than RETENTION_MONTHS to a cold folder."""
    bucket = storage_client.bucket(bucket_name)
    cutoff_date = datetime.date.today() - relativedelta(months=RETENTION_MONTHS)
    prefix = f"{hot_folder}{ticker}_"
    
    # Regex to extract date from filenames like 'AAPL_2023-09-30.json'
    pattern = re.compile(rf"{ticker}_(\d{{4}}-\d{{2}}-\d{{2}})\.json$")

    for blob in bucket.list_blobs(prefix=prefix):
        match = pattern.search(blob.name)
        if not match:
            continue
        try:
            file_date = datetime.date.fromisoformat(match.group(1))
            if file_date < cutoff_date:
                destination_path = blob.name.replace(hot_folder, cold_folder)
                logging.info(f"Archiving {blob.name} to {destination_path}")
                bucket.copy_blob(blob, bucket, destination_path)
                blob.delete()
        except (ValueError, IndexError):
            logging.warning(f"Could not parse date from blob name: {blob.name}")
            continue