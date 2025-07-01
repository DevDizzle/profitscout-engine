# transcript_collector/core/orchestrator.py
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import (
    MAX_WORKERS, GCS_BUCKET_NAME, GCS_OUTPUT_FOLDER
)
from core.gcs import get_tickers, blob_exists, upload_json_to_gcs
from core.client import FMPClient
from google.cloud import storage

def process_ticker(ticker: str, fmp_client: FMPClient, storage_client: storage.Client):
    """Applies the hybrid update logic for a single ticker's transcript."""
    # Fetch the entire period object from the API
    latest_period = fmp_client.get_latest_period_info(ticker)
    if not latest_period:
        return f"{ticker}: No latest period info found, skipped."

    # Extract date, year, and quarter directly from the API response
    latest_date = latest_period.get("date")
    year = latest_period.get("year")
    quarter = latest_period.get("quarter")

    if not all([latest_date, year, quarter]):
         return f"{ticker}: API response was missing date, year, or quarter. Skipped."

    expected_filename = f"{GCS_OUTPUT_FOLDER}{ticker}_{latest_date}.json"

    if blob_exists(storage_client, GCS_BUCKET_NAME, expected_filename):
        return f"{ticker}: Latest transcript ({latest_date}) already exists."

    logging.info(f"{ticker}: Transcript for {latest_date} is missing. Fetching...")
    transcript_data = fmp_client.fetch_transcript(ticker, year, quarter)

    if not (transcript_data and transcript_data.get("content")):
        return f"{ticker}: No transcript content found for {year} Q{quarter}."

    upload_json_to_gcs(storage_client, GCS_BUCKET_NAME, transcript_data, expected_filename)
    
    # The cleanup_old_files call was removed to ensure all data is preserved.
    
    return f"{ticker}: Transcript for {year} Q{quarter} uploaded."

def run_pipeline(fmp_client: FMPClient, storage_client: storage.Client):
    """Runs the full transcript collection pipeline."""
    tickers = get_tickers(storage_client, GCS_BUCKET_NAME)
    if not tickers:
        logging.error("No tickers found. Exiting.")
        return

    logging.info(f"Starting transcript collection for {len(tickers)} tickers.")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ticker, t, fmp_client, storage_client): t for t in tickers}
        for future in as_completed(futures):
            try:
                result = future.result()
                logging.info(result)
            except Exception as e:
                ticker = futures[future]
                logging.error(f"{ticker}: An error occurred: {e}", exc_info=True)

    logging.info("Transcript collection pipeline complete.")