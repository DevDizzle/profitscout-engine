import requests
import os
import logging
from google.cloud import storage
from math import ceil
import json

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Fetches configuration from environment variables.
FMP_API_KEY = os.getenv('FMP_API_KEY')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'profit-scout-data')
TICKER_LIST_PATH = "tickerlist.txt"
DESTINATION_FOLDER = "images/"

# Batch size for API requests.
BATCH_SIZE = 50

# --- Helper Functions ---

def get_tickers_from_gcs(bucket_name: str, blob_path: str) -> list[str]:
    """Loads the official ticker list from the GCS bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        if not blob.exists():
            logging.error(f"Ticker file not found in GCS: gs://{bucket_name}/{blob_path}")
            return []
        
        content = blob.download_as_text(encoding="utf-8")
        tickers = [line.strip().upper() for line in content.splitlines() if line.strip()]
        logging.info(f"Successfully loaded {len(tickers)} tickers from GCS.")
        return tickers
    except Exception as e:
        logging.critical(f"Failed to load tickers from GCS: {e}", exc_info=True)
        return []

def upload_to_gcs(bucket_name: str, source_file_path: str, destination_blob_name: str) -> str | None:
    """Uploads a local image file to GCS and returns its GCS URI."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        blob.upload_from_filename(source_file_path)
        
        # Return the GCS URI for use with signed URLs.
        return f"gs://{bucket_name}/{destination_blob_name}"
    except Exception as e:
        logging.error(f"Failed to upload {source_file_path} to GCS: {e}", exc_info=True)
        return None

# --- Main Execution Logic ---

def main():
    """Main function to orchestrate the fetching and uploading of company logos."""
    if not FMP_API_KEY:
        logging.critical("FMP_API_KEY environment variable not set. Please set it before running.")
        return

    tickers = get_tickers_from_gcs(GCS_BUCKET_NAME, TICKER_LIST_PATH)
    if not tickers:
        logging.critical("No tickers loaded. Exiting.")
        return

    num_batches = ceil(len(tickers) / BATCH_SIZE)
    uri_map = {}
    logging.info(f"Starting to process {len(tickers)} tickers in {num_batches} batches.")

    for i in range(num_batches):
        batch_start = i * BATCH_SIZE
        batch_end = batch_start + BATCH_SIZE
        batch_tickers = tickers[batch_start:batch_end]
        tickers_str = ','.join(batch_tickers)
        
        logging.info(f"--- Processing Batch {i+1}/{num_batches} ---")
        
        api_url = f"https://financialmodelingprep.com/api/v3/profile/{tickers_str}?apikey={FMP_API_KEY}"
        try:
            response = requests.get(api_url, timeout=20)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            logging.error(f"API request failed for batch {i+1}: {e}")
            continue
        
        for item in data:
            ticker = item.get('symbol')
            logo_url = item.get('image')
            
            if not ticker or not logo_url:
                logging.warning(f"Skipping item due to missing ticker or logo URL: {item}")
                continue
            
            try:
                img_response = requests.get(logo_url, timeout=20)
                img_response.raise_for_status()
            except requests.RequestException as e:
                logging.error(f"Failed to download logo for {ticker}: {e}")
                continue
            
            ext = os.path.splitext(logo_url)[1] or '.png'
            local_file = f"{ticker}{ext}"
            
            with open(local_file, 'wb') as f:
                f.write(img_response.content)
            
            blob_name = f"{DESTINATION_FOLDER}{ticker}{ext}"
            gcs_uri = upload_to_gcs(GCS_BUCKET_NAME, local_file, blob_name)
            
            if gcs_uri:
                uri_map[ticker] = gcs_uri
                logging.info(f"Successfully uploaded logo for {ticker}: {gcs_uri}")
            
            os.remove(local_file)

    if uri_map:
        output_filename = 'ticker_uris.json'
        with open(output_filename, 'w') as f:
            json.dump(uri_map, f, indent=4)
        logging.info(f"Saved URI map for {len(uri_map)} tickers to {output_filename}.")

    logging.info("--- Image fetching process complete! ---")

if __name__ == "__main__":
    main()