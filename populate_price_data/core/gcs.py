import logging
from google.cloud import storage
from config import GCS_BUCKET_NAME, TICKER_FILE_PATH

def get_tickers_from_gcs(storage_client: storage.Client) -> list[str]:
    """Reads a list of tickers from a file in GCS."""
    try:
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(TICKER_FILE_PATH)
        if not blob.exists():
            logging.error(f"Ticker file not found: {TICKER_FILE_PATH}")
            return []
        content = blob.download_as_text(encoding="utf-8").strip()
        return [line.strip().upper() for line in content.split("\n") if line.strip()]
    except Exception as e:
        logging.error(f"Failed to read ticker file from GCS: {e}")        return []