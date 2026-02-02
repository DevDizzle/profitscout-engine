# ingestion/core/orchestrators/fundamentals.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage

from .. import config
from ..clients.fmp_client import FMPClient
from ..gcs import blob_exists, cleanup_old_files, get_tickers, upload_json_to_gcs


def _is_data_incomplete(data) -> bool:
    """
    Checks if the most recent record appears to be a placeholder (e.g. zero revenue & zero OCF).
    """
    if not data or not isinstance(data, list):
        return True  # Empty or invalid is 'incomplete'

    # FMP usually sorts descending by date. Check the latest.
    latest = data[0]

    # Critical checks: A real operating company should not have exactly 0 for both.
    # We use a strict 0 check because FMP placeholders are exactly 0.
    rev = latest.get("revenuePerShare", 0)
    ocf = latest.get("operatingCashFlowPerShare", 0)

    return bool(rev == 0 and ocf == 0)


def process_ticker(ticker: str, fmp_client: FMPClient, storage_client: storage.Client):
    latest_date = fmp_client.get_latest_quarter_end_date(ticker)
    if not latest_date:
        return f"{ticker}: No latest date found, skipped."

    endpoints = {
        "key-metrics": config.KEY_METRICS_FOLDER,
        "ratios": config.RATIOS_FOLDER,
    }

    for endpoint_name, gcs_folder in endpoints.items():
        expected_filename = f"{gcs_folder}{ticker}_{latest_date}.json"
        fetch_needed = True

        if blob_exists(storage_client, expected_filename):
            # TRUST BUT VERIFY: Check if the existing file is a "placeholder" stub
            from ..gcs import read_blob  # lazy import

            existing_json_str = read_blob(config.GCS_BUCKET_NAME, expected_filename)

            import json

            try:
                existing_data = (
                    json.loads(existing_json_str) if existing_json_str else []
                )
                if _is_data_incomplete(existing_data):
                    logging.warning(
                        f"{ticker} ({endpoint_name}): Existing file found but data is INCOMPLETE (zeros). Forcing refresh."
                    )
                    fetch_needed = True
                else:
                    logging.info(
                        f"{ticker} ({endpoint_name}) is up-to-date and complete."
                    )
                    fetch_needed = False
            except Exception as e:
                logging.warning(
                    f"{ticker}: Failed to validate existing file {expected_filename}: {e}. Refreshing."
                )
                fetch_needed = True

        if fetch_needed:
            logging.info(f"{ticker} ({endpoint_name}) fetching new data...")
            data = fmp_client.get_financial_data(
                ticker, endpoint_name, limit=config.QUARTERS_TO_FETCH
            )
            if not data:
                logging.warning(f"{ticker}: No {endpoint_name} data returned from API.")
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
        futures = {
            executor.submit(process_ticker, t, fmp_client, storage_client): t
            for t in tickers
        }
        for future in as_completed(futures):
            try:
                logging.info(future.result())
            except Exception as e:
                logging.error(
                    f"'{futures[future]}': An error occurred: {e}", exc_info=True
                )
    logging.info("Fundamentals refresh pipeline complete.")
