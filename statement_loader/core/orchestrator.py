import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from config import (
    MAX_WORKERS, GCS_BUCKET_NAME, QUARTERS_TO_FETCH, GCS_OUTPUT_FOLDER
)
from core.gcs import get_tickers, blob_exists, upload_json_to_gcs, cleanup_old_files
from core.client import FMPClient
from google.cloud import storage

def find_match_by_date(data_list: list[dict], target_date: str) -> dict:
    """Finds the record in a list that matches a specific date."""
    return next((item for item in data_list if item.get("date") == target_date), {})

def process_ticker(ticker: str, fmp_client: FMPClient, storage_client: storage.Client):
    """Applies the hybrid update logic for a single ticker's statements."""
    latest_date = fmp_client.get_latest_quarter_end_date(ticker)
    if not latest_date:
        return f"{ticker}: No latest date found, skipped."

    expected_filename = f"{GCS_OUTPUT_FOLDER}{ticker}_{latest_date}.json"

    if blob_exists(storage_client, GCS_BUCKET_NAME, expected_filename):
        return f"{ticker}: Financial statements are already up-to-date."

    logging.info(f"{ticker}: Statements are outdated. Fetching new data...")
    all_data = fmp_client.get_financial_statements(ticker, limit=QUARTERS_TO_FETCH)

    if not any(all_data.values()):
        return f"{ticker}: No financial statement data returned from API."

    # Get all unique dates from the fetched data
    all_dates = sorted(pd.to_datetime(
        [item['date'] for statement_list in all_data.values() for item in statement_list]
    ).unique(), reverse=True)

    quarterly_reports = []
    for date_obj in all_dates:
        date_str = date_obj.strftime('%Y-%m-%d')
        report = {
            "date": date_str,
            "income_statement": find_match_by_date(all_data.get('income', []), date_str),
            "balance_sheet": find_match_by_date(all_data.get('balance', []), date_str),
            "cash_flow_statement": find_match_by_date(all_data.get('cashflow', []), date_str)
        }
        quarterly_reports.append(report)

    output_doc = {"symbol": ticker, "quarterly_reports": quarterly_reports}

    upload_json_to_gcs(storage_client, GCS_BUCKET_NAME, output_doc, expected_filename)
    cleanup_old_files(storage_client, GCS_BUCKET_NAME, GCS_OUTPUT_FOLDER, ticker, expected_filename)

    return f"{ticker}: Statements processed and uploaded."


def run_pipeline(fmp_client: FMPClient, storage_client: storage.Client):
    """Runs the full statement loader pipeline."""
    tickers = get_tickers(storage_client, GCS_BUCKET_NAME)
    if not tickers:
        logging.error("No tickers found. Exiting.")
        return

    logging.info(f"Starting statement load for {len(tickers)} tickers.")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ticker, t, fmp_client, storage_client): t for t in tickers}
        for future in as_completed(futures):
            try:
                result = future.result()
                logging.info(result)
            except Exception as e:
                ticker = futures[future]
                logging.error(f"{ticker}: An error occurred: {e}", exc_info=True)
    logging.info("Statement loader pipeline complete.")