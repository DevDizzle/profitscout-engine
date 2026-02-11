# ingestion/core/orchestrators/statement_loader.py
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage

from .. import config
from ..clients.fmp_client import FMPClient
from ..gcs import blob_exists, cleanup_old_files, get_tickers, upload_json_to_gcs


def find_match_by_date(data_list: list[dict], target_date: str) -> dict:
    """Finds the record in a list that matches a specific date."""
    return next((item for item in data_list if item.get("date") == target_date), {})


def _is_latest_quarter_incomplete(quarterly_reports: list[dict]) -> bool:
    """
    Checks if the most recent financial statement record appears to be incomplete (missing revenue/assets).
    """
    if not quarterly_reports:
        return False  # No data is "incomplete" in a way, but we can't check zeros.

    latest = quarterly_reports[0] # Assumes sorted descending
    
    inc = latest.get("income_statement", {})
    bal = latest.get("balance_sheet", {})
    cf = latest.get("cash_flow_statement", {})

    # Check for placeholder zeros in key fields
    revenue = inc.get("revenue", -1)
    assets = bal.get("totalAssets", -1)
    ocf = cf.get("operatingCashFlow", -1)

    if revenue == 0 or assets == 0 or ocf == 0:
        return True
    
    return False


def process_ticker(ticker: str, fmp_client: FMPClient, storage_client: storage.Client):
    """
    Fetches Income, Balance Sheet, and Cash Flow statements, merges them by quarter,
    and saves a single consolidated JSON file per ticker.
    """
    # 1. Fetch data from all 3 endpoints
    try:
        income_data = fmp_client.get_financial_data(ticker, "income-statement", limit=config.QUARTERS_TO_FETCH) or []
        balance_data = fmp_client.get_financial_data(ticker, "balance-sheet-statement", limit=config.QUARTERS_TO_FETCH) or []
        cash_flow_data = fmp_client.get_financial_data(ticker, "cash-flow-statement", limit=config.QUARTERS_TO_FETCH) or []
    except Exception as e:
        logging.error(f"{ticker}: Failed to fetch financial data: {e}")
        return f"{ticker}: Fetch failed."

    if not income_data:
        logging.warning(f"{ticker}: No income statement data found. Skipping.")
        return f"{ticker}: No data."

    # 2. Determine latest date for filename
    # Income data is usually the most reliable driver.
    latest_date = income_data[0].get("date")
    if not latest_date:
        logging.warning(f"{ticker}: Could not determine latest date from income statement.")
        return f"{ticker}: No date."

    expected_filename = f"{config.FINANCIAL_STATEMENTS_FOLDER}{ticker}_{latest_date}.json"

    # 3. Check existence and completeness (Trust but Verify)
    if blob_exists(storage_client, expected_filename):
        from ..gcs import read_blob
        try:
            existing_content = read_blob(config.GCS_BUCKET_NAME, expected_filename)
            existing_json = json.loads(existing_content)
            
            # Check if existing data is "complete"
            if not _is_latest_quarter_incomplete(existing_json.get("quarterly_reports", [])):
                logging.info(f"{ticker}: Data up-to-date and complete ({latest_date}). Skipping.")
                return f"{ticker}: Skipped (Up-to-date)."
            else:
                logging.warning(f"{ticker}: Existing data found but INCOMPLETE. Refreshing.")
        except Exception as e:
            logging.warning(f"{ticker}: Failed to validate existing file. Refreshing. Error: {e}")

    # 4. Merge Data
    # Collect all unique dates from all statements
    all_dates = set()
    all_dates.update(d.get("date") for d in income_data)
    all_dates.update(d.get("date") for d in balance_data)
    all_dates.update(d.get("date") for d in cash_flow_data)
    
    sorted_dates = sorted(list(all_dates), reverse=True)
    
    quarterly_reports = []
    for d in sorted_dates:
        report = {
            "date": d,
            "income_statement": find_match_by_date(income_data, d),
            "balance_sheet": find_match_by_date(balance_data, d),
            "cash_flow_statement": find_match_by_date(cash_flow_data, d)
        }
        # Only add if we have at least one statement
        if report["income_statement"] or report["balance_sheet"] or report["cash_flow_statement"]:
            quarterly_reports.append(report)

    final_payload = {
        "symbol": ticker,
        "quarterly_reports": quarterly_reports
    }

    # 5. Save to GCS
    upload_json_to_gcs(storage_client, final_payload, expected_filename)
    
    # 6. Cleanup old files
    cleanup_old_files(storage_client, config.FINANCIAL_STATEMENTS_FOLDER, ticker, expected_filename)

    return f"{ticker}: Statements merged and saved."


def run_pipeline(fmp_client: FMPClient, storage_client: storage.Client):
    """Runs the full statement loader pipeline."""
    tickers = get_tickers(storage_client)
    if not tickers:
        logging.error("No tickers found. Exiting statement loader pipeline.")
        return

    logging.info(f"Starting aggregated statement load for {len(tickers)} tickers.")
    max_workers = config.MAX_WORKERS_TIERING.get("statement_loader") or 5
    
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