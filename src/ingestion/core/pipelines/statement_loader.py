# ingestion/core/orchestrators/statement_loader.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage

from .. import config
from ..clients.fmp_client import FMPClient
from ..gcs import blob_exists, cleanup_old_files, get_tickers, upload_json_to_gcs


def find_match_by_date(data_list: list[dict], target_date: str) -> dict:
    """Finds the record in a list that matches a specific date."""
    return next((item for item in data_list if item.get("date") == target_date), {})


def _is_statement_incomplete(data) -> bool:
    """
    Checks if the most recent financial statement record appears to be a placeholder (zeros).
    """
    if not data or not isinstance(data, list):
        return True

    latest = data[0]

    # Check key fields across different statement types.
    # We use .get() so this works for Income, Balance Sheet, and Cash Flow.
    revenue = latest.get(
        "revenue", -1
    )  # Default -1 to ignore if field invalid for this type
    assets = latest.get("totalAssets", -1)
    ocf = latest.get("operatingCashFlow", -1)

    # Heuristic: If it's an Income Statement (has revenue) and revenue is 0.
    if revenue == 0:
        return True

    # Heuristic: If it's a Balance Sheet (has assets) and assets are 0.
    if assets == 0:
        return True

    # Heuristic: If it's a Cash Flow Statement (has OCF) and OCF is 0.
    return ocf == 0


def process_ticker(ticker: str, fmp_client: FMPClient, storage_client: storage.Client):
    """
    Orchestrates the fetch and load of Income, Balance Sheet, and Cash Flow statements.
    """
    endpoints = {
        "income-statement": config.INCOME_STATEMENT_FOLDER,
        "balance-sheet-statement": config.BALANCE_SHEET_FOLDER,
        "cash-flow-statement": config.CASH_FLOW_FOLDER,
    }

    # We need a reference date to name the files correctly.
    # Usually we use the latest quarter date from the API.
    latest_date = fmp_client.get_latest_quarter_end_date(ticker)
    if not latest_date:
        return f"{ticker}: No latest date found (statements), skipped."

    for endpoint_name, gcs_folder in endpoints.items():
        expected_filename = f"{gcs_folder}{ticker}_{latest_date}.json"
        fetch_needed = True

        if blob_exists(storage_client, expected_filename):
            # TRUST BUT VERIFY
            from ..gcs import read_blob

            existing_json_str = read_blob(config.GCS_BUCKET_NAME, expected_filename)
            import json

            try:
                existing_data = (
                    json.loads(existing_json_str) if existing_json_str else []
                )
                if _is_statement_incomplete(existing_data):
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
                    f"{ticker}: Failed to validate existing statement {expected_filename}: {e}. Refreshing."
                )
                fetch_needed = True

        if fetch_needed:
            logging.info(f"{ticker} ({endpoint_name}) fetching new data...")
            data = fmp_client.get_financial_data(
                ticker, endpoint_name, limit=config.QUARTERS_TO_FETCH
            )
            if not data:
                logging.warning(f"{ticker}: No {endpoint_name} data returned.")
                continue

            upload_json_to_gcs(storage_client, data, expected_filename)
            cleanup_old_files(storage_client, gcs_folder, ticker, expected_filename)

    return f"{ticker}: Statements processing complete."


def run_pipeline(fmp_client: FMPClient, storage_client: storage.Client):
    """Runs the full statement loader pipeline."""
    tickers = get_tickers(storage_client)
    if not tickers:
        logging.error("No tickers found. Exiting statement loader pipeline.")
        return

    logging.info(f"Starting statement load for {len(tickers)} tickers.")
    max_workers = config.MAX_WORKERS_TIERING.get("statement_loader")
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
    logging.info("Statement loader pipeline complete.")
