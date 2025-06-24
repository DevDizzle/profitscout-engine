# price_updater/core/orchestrator.py
import logging
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import MAX_WORKERS, GCS_BUCKET_NAME, GCS_OUTPUT_FOLDER
from core.gcs import get_tickers, upload_json_to_gcs
from core.client import FMPClient
from google.cloud import storage

def process_ticker(ticker: str, fmp_client: FMPClient, storage_client: storage.Client):
    """Fetches, formats, and uploads 90-day price data for a single ticker."""
    price_records = fmp_client.fetch_90_day_prices(ticker)

    if not price_records:
        return f"{ticker}: No price data returned from API."

    # Format the final JSON document
    output_doc = {
        "ticker": ticker,
        "as_of_date": datetime.date.today().isoformat(),
        "prices": price_records
    }

    # Construct the new, flattened blob path
    blob_path = f"{GCS_OUTPUT_FOLDER}{ticker}_90_day_prices.json"

    upload_json_to_gcs(storage_client, GCS_BUCKET_NAME, output_doc, blob_path)
    return f"{ticker}: Price snapshot uploaded successfully."

def run_pipeline(fmp_client: FMPClient, storage_client: storage.Client):
    """Runs the full price updater pipeline."""
    tickers = get_tickers(storage_client, GCS_BUCKET_NAME)
    if not tickers:
        logging.error("No tickers found. Exiting.")
        return

    logging.info(f"Starting 90-day price update for {len(tickers)} tickers.")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ticker, t, fmp_client, storage_client): t for t in tickers}
        for future in as_completed(futures):
            try:
                result = future.result()
                logging.info(result)
            except Exception as e:
                ticker = futures[future]
                logging.error(f"{ticker}: An error occurred: {e}", exc_info=True)

    logging.info("Price updater pipeline complete.")