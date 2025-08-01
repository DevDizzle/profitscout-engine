import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import (
    MAX_WORKERS, GCS_BUCKET_NAME, SECTION_MAP,
    BUSINESS_FOLDER, MDA_FOLDER, RISK_FOLDER
)
from core.gcs import get_tickers, blob_exists, upload_json_to_gcs, cleanup_old_files
from core.client import SecApiClient
from google.cloud import storage

def _extract_and_save_section(
    client: SecApiClient, storage_client: storage.Client, filing: dict, 
    section_name: str, output_folder: str, is_versioned: bool
):
    """Helper to extract, save, and clean up a single section."""
    form_type = filing["formType"]
    section_key = SECTION_MAP.get(form_type, {}).get(section_name)
    if not section_key:
        return

    ticker = filing["ticker"]
    
    if is_versioned:
        date_iso = filing["periodOfReport"][:10]
        output_filename = f"{output_folder}{ticker}_{date_iso}_{form_type}.json"
        if blob_exists(storage_client, GCS_BUCKET_NAME, output_filename):
            logging.info(f"{ticker}: {section_name} for {date_iso} already exists.")
            return
    else:
        output_filename = f"{output_folder}{ticker}_business_profile.json"

    content = client.extract_section(filing["linkToFilingDetails"], section_key)
    if not content:
        return

    upload_json_to_gcs(storage_client, GCS_BUCKET_NAME, {section_name: content}, output_filename)
    
    if is_versioned:
        cleanup_old_files(storage_client, GCS_BUCKET_NAME, output_folder, ticker, output_filename)

def process_ticker(ticker: str, client: SecApiClient, storage_client: storage.Client):
    """Fetches latest filings and extracts all required sections."""
    filings = client.get_latest_filings(ticker)

    # Process latest annual filing (10-K, etc.)
    annual_filing = filings.get("annual")
    if annual_filing:
        logging.info(f"Processing annual filing for {ticker} from {annual_filing['filedAt'][:10]}")
        _extract_and_save_section(client, storage_client, annual_filing, "business", BUSINESS_FOLDER, is_versioned=False)
        _extract_and_save_section(client, storage_client, annual_filing, "mda", MDA_FOLDER, is_versioned=True)
        _extract_and_save_section(client, storage_client, annual_filing, "risk", RISK_FOLDER, is_versioned=True)

    # Process latest quarterly filing (10-Q)
    quarterly_filing = filings.get("quarterly")
    if quarterly_filing:
        logging.info(f"Processing quarterly filing for {ticker} from {quarterly_filing['filedAt'][:10]}")
        _extract_and_save_section(client, storage_client, quarterly_filing, "mda", MDA_FOLDER, is_versioned=True)
        _extract_and_save_section(client, storage_client, quarterly_filing, "risk", RISK_FOLDER, is_versioned=True)

    return f"{ticker}: SEC filing extraction complete."

def run_pipeline(client: SecApiClient, storage_client: storage.Client):
    """Runs the full SEC filing extraction pipeline."""
    tickers = get_tickers(storage_client, GCS_BUCKET_NAME)
    if not tickers:
        logging.error("No tickers found. Exiting.")
        return

    logging.info(f"Starting SEC extraction for {len(tickers)} tickers.")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ticker, t, client, storage_client): t for t in tickers}
        for future in as_completed(futures):
            try:
                result = future.result()
                logging.info(result)
            except Exception as e:
                ticker = futures[future]
                logging.error(f"{ticker}: An error occurred: {e}", exc_info=True)
    
    logging.info("SEC filing extraction pipeline complete.")