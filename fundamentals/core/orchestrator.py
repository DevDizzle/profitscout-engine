import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import (
    MAX_WORKERS, GCS_BUCKET_NAME, QUARTERS_TO_FETCH,
    KEY_METRICS_FOLDER, RATIOS_FOLDER
)
from core.gcs import get_tickers, blob_exists, upload_json_to_gcs, cleanup_old_files
from core.client import FMPClient
from google.cloud import storage

def process_ticker(ticker: str, fmp_client: FMPClient, storage_client: storage.Client):
    """Applies the hybrid update logic for a single ticker."""
    latest_date = fmp_client.get_latest_quarter_end_date(ticker)
    if not latest_date:
        return f"{ticker}: No latest date found, skipped."

    endpoints = {
        "key-metrics": KEY_METRICS_FOLDER,
        "ratios": RATIOS_FOLDER
    }

    for endpoint_name, gcs_folder in endpoints.items():
        expected_filename = f"{gcs_folder}{ticker}_{latest_date}.json"

        if blob_exists(storage_client, GCS_BUCKET_NAME, expected_filename):
            logging.info(f"{ticker} ({endpoint_name}) is already up-to-date.")
            continue

        logging.info(f"{ticker} ({endpoint_name}) is outdated. Fetching new data...")
        data = fmp_client.get_financial_data(ticker, endpoint_name, limit=QUARTERS_TO_FETCH)

        if not data:
            logging.warning(f"{ticker}: No {endpoint_name} data returned from API.")
            continue

        upload_json_to_gcs(storage_client, GCS_BUCKET_NAME, data, expected_filename)
        cleanup_old_files(storage_client, GCS_BUCKET_NAME, gcs_folder, ticker, expected_filename)

    return f"{ticker}: Processing complete."

def run_pipeline(fmp_client: FMPClient, storage_client: storage.Client):
    """Runs the full fundamentals pipeline using the hybrid model."""
    tickers = get_tickers(storage_client, GCS_BUCKET_NAME)
    if not tickers:
        logging.error("No tickers found. Exiting.")
        return

    logging.info(f"Starting fundamentals refresh for {len(tickers)} tickers.")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {executor.submit(process_ticker, t, fmp_client, storage_client): t for t in tickers}
        for future in as_completed(future_to_ticker):
            try:
                result = future.result()
                logging.info(result)
            except Exception as e:
                ticker = future_to_ticker[future]
                logging.error(f"'{ticker}': An error occurred during processing: {e}", exc_info=True)

    logging.info("Fundamentals refresh pipeline complete.")