# ingestion/core/orchestrators/fundamentals.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from .. import config
from ..gcs import get_tickers, blob_exists, upload_json_to_gcs, cleanup_old_files
from ..clients.fmp_client import FMPClient

def process_ticker(ticker: str, fmp_client: FMPClient, storage_client: storage.Client):
    latest_date = fmp_client.get_latest_quarter_end_date(ticker)
    if not latest_date:
        return f"{ticker}: No latest date found, skipped."

    endpoints = {"key-metrics": config.KEY_METRICS_FOLDER, "ratios": config.RATIOS_FOLDER}
    for endpoint_name, gcs_folder in endpoints.items():
        expected_filename = f"{gcs_folder}{ticker}_{latest_date}.json"
        if blob_exists(storage_client, expected_filename):
            logging.info(f"{ticker} ({endpoint_name}) is already up-to-date.")
            continue

        logging.info(f"{ticker} ({endpoint_name}) is outdated. Fetching new data...")
        data = fmp_client.get_financial_data(ticker, endpoint_name, limit=config.QUARTERS_TO_FETCH)
        if not data:
            logging.warning(f"{ticker}: No {endpoint_name} data returned.")
            continue

        upload_json_to_gcs(storage_client, data, expected_filename)
        cleanup_old_files(storage_client, gcs_folder, ticker, expected_filename)
    return f"{ticker}: Fundamentals processing complete."

def run_pipeline(fmp_client: FMPClient, storage_client: storage.Client):
    tickers = get_tickers(storage_client)
    if not tickers:
        logging.error("No tickers found. Exiting fundamentals pipeline.")
        return

    logging.info(f"Starting fundamentals refresh for {len(tickers)} tickers.")
    max_workers = config.MAX_WORKERS_TIERING.get("fundamentals")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_ticker, t, fmp_client, storage_client): t for t in tickers}
        for future in as_completed(futures):
            try:
                logging.info(future.result())
            except Exception as e:
                logging.error(f"'{futures[future]}': An error occurred: {e}", exc_info=True)
    logging.info("Fundamentals refresh pipeline complete.")