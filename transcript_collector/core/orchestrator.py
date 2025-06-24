# transcript_collector/core/orchestrator.py
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import (
    MAX_WORKERS, GCS_BUCKET_NAME, GCS_OUTPUT_FOLDER
)
from core.gcs import get_tickers, blob_exists, upload_json_to_gcs, cleanup_old_files
from core.client import FMPClient
from google.cloud import storage

def get_fiscal_quarter(ticker: str, end_date_str: str) -> tuple[int, int]:
    """Calculates the fiscal quarter and year from a date string."""
    d = pd.to_datetime(end_date_str)
    # Special handling for Apple's fiscal calendar
    if ticker == "AAPL":
        if d.month >= 10: return d.year + 1, 1
        if d.month <= 3: return d.year, 2
        if d.month <= 6: return d.year, 3
        return d.year, 4
    # Standard calendar quarter
    return d.year, (d.month - 1) // 3 + 1

def process_ticker(ticker: str, fmp_client: FMPClient, storage_client: storage.Client):
    """Applies the hybrid update logic for a single ticker's transcript."""
    latest_date = fmp_client.get_latest_quarter_end_date(ticker)
    if not latest_date:
        return f"{ticker}: No latest quarter date found, skipped."

    expected_filename = f"{GCS_OUTPUT_FOLDER}{ticker}_{latest_date}.json"

    if blob_exists(storage_client, GCS_BUCKET_NAME, expected_filename):
        return f"{ticker}: Latest transcript ({latest_date}) already exists."

    logging.info(f"{ticker}: Transcript for {latest_date} is missing. Fetching...")
    year, quarter = get_fiscal_quarter(ticker, latest_date)
    transcript_data = fmp_client.fetch_transcript(ticker, year, quarter)

    if not (transcript_data and transcript_data.get("content")):
        return f"{ticker}: No transcript content found for {year} Q{quarter}."

    upload_json_to_gcs(storage_client, GCS_BUCKET_NAME, transcript_data, expected_filename)
    
    # Clean up old files after the new one is successfully uploaded
    cleanup_old_files(storage_client, GCS_BUCKET_NAME, GCS_OUTPUT_FOLDER, ticker, expected_filename)
    
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