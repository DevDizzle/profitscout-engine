# ingestion/core/orchestrators/sec_filing_extractor.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from .. import config
from ..gcs import get_tickers, blob_exists, upload_json_to_gcs, cleanup_old_files
from ..clients.sec_api_client import SecApiClient

def _extract_and_save_section(client: SecApiClient, storage_client: storage.Client, filing: dict, section_name: str, output_folder: str, is_versioned: bool):
    form_type = filing["formType"]
    section_key = config.SECTION_MAP.get(form_type, {}).get(section_name)
    if not section_key: return

    ticker = filing["ticker"]
    if is_versioned:
        date_iso = filing["periodOfReport"][:10]
        output_filename = f"{output_folder}{ticker}_{date_iso}_{form_type}.json"
        if blob_exists(storage_client, output_filename):
            logging.info(f"{ticker}: {section_name} for {date_iso} already exists.")
            return
    else:
        output_filename = f"{output_folder}{ticker}_business_profile.json"

    content = client.extract_section(filing["linkToFilingDetails"], section_key)
    if not content: return

    upload_json_to_gcs(storage_client, {section_name: content}, output_filename)
    if is_versioned:
        cleanup_old_files(storage_client, output_folder, ticker, output_filename)

def process_ticker(ticker: str, client: SecApiClient, storage_client: storage.Client):
    filings = client.get_latest_filings(ticker)
    if annual_filing := filings.get("annual"):
        logging.info(f"Processing annual filing for {ticker} from {annual_filing['filedAt'][:10]}")
        _extract_and_save_section(client, storage_client, annual_filing, "business", config.BUSINESS_FOLDER, is_versioned=False)
        _extract_and_save_section(client, storage_client, annual_filing, "mda", config.MDA_FOLDER, is_versioned=True)
        _extract_and_save_section(client, storage_client, annual_filing, "risk", config.RISK_FOLDER, is_versioned=True)
    if quarterly_filing := filings.get("quarterly"):
        logging.info(f"Processing quarterly filing for {ticker} from {quarterly_filing['filedAt'][:10]}")
        _extract_and_save_section(client, storage_client, quarterly_filing, "mda", config.MDA_FOLDER, is_versioned=True)
        _extract_and_save_section(client, storage_client, quarterly_filing, "risk", config.RISK_FOLDER, is_versioned=True)
    return f"{ticker}: SEC filing extraction complete."

def run_pipeline(client: SecApiClient, storage_client: storage.Client):
    tickers = get_tickers(storage_client)
    if not tickers:
        logging.error("No tickers found. Exiting SEC pipeline.")
        return

    logging.info(f"Starting SEC extraction for {len(tickers)} tickers.")
    max_workers = config.MAX_WORKERS_TIERING.get("sec_filing_extractor")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_ticker, t, client, storage_client): t for t in tickers}
        for future in as_completed(futures):
            try:
                logging.info(future.result())
            except Exception as e:
                logging.error(f"'{futures[future]}': An error occurred: {e}", exc_info=True)
    logging.info("SEC filing extraction pipeline complete.")