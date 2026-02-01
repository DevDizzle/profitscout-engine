# ingestion/core/orchestrators/sec_filing_extractor.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage

from .. import config
from ..clients.sec_api_client import SecApiClient
from ..gcs import cleanup_old_files, get_tickers, upload_json_to_gcs


def _extract_and_save_section(
    client: SecApiClient,
    storage_client: storage.Client,
    filing: dict,
    section_name: str,
    output_folder: str,
):
    """
    Extracts a single section, saves it with the new naming convention,
    and cleans up any older versions for that ticker in the same folder.
    """
    form_type = filing.get("formType")
    ticker = filing.get("ticker")
    section_key = config.SECTION_MAP.get(form_type, {}).get(section_name)

    if not all([form_type, ticker, section_key]):
        logging.warning(
            f"Skipping section '{section_name}' due to missing data in filing: {filing}"
        )
        return

    # --- New Naming Convention ---
    # The filename is now always TICKER_YYYY-MM-DD.json based on the reporting period.
    date_iso = filing["periodOfReport"][:10]
    output_filename = f"{output_folder}{ticker}_{date_iso}.json"

    # --- Extract and Upload ---
    logging.info(f"Extracting '{section_name}' for {ticker} from {date_iso} filing.")
    content = client.extract_section(filing["linkToFilingDetails"], section_key)

    if not content:
        logging.warning(f"No content found for section '{section_name}' for {ticker}.")
        return

    upload_json_to_gcs(storage_client, {section_name: content}, output_filename)

    # --- Crucial Step: Clean up old files ---
    # This ensures only the file we just uploaded remains for this ticker in this folder.
    cleanup_old_files(storage_client, output_folder, ticker, output_filename)


def process_ticker(ticker: str, client: SecApiClient, storage_client: storage.Client):
    """
    Fetches the latest annual and quarterly filings and processes each required
    section to ensure only the most recent version is stored.
    """
    filings = client.get_latest_filings(ticker)

    # --- Process latest ANNUAL filing (10-K, etc.) ---
    if annual_filing := filings.get("annual"):
        logging.info(
            f"Processing ANNUAL filing for {ticker} from {annual_filing['filedAt'][:10]}"
        )
        # Business section is only in annual reports
        _extract_and_save_section(
            client, storage_client, annual_filing, "business", config.BUSINESS_FOLDER
        )
        _extract_and_save_section(
            client, storage_client, annual_filing, "mda", config.MDA_FOLDER
        )
        _extract_and_save_section(
            client, storage_client, annual_filing, "risk", config.RISK_FOLDER
        )

    # --- Process latest QUARTERLY filing (10-Q) ---
    # This will overwrite the MDA and Risk sections if the 10-Q is more recent
    if quarterly_filing := filings.get("quarterly"):
        logging.info(
            f"Processing QUARTERLY filing for {ticker} from {quarterly_filing['filedAt'][:10]}"
        )
        _extract_and_save_section(
            client, storage_client, quarterly_filing, "mda", config.MDA_FOLDER
        )
        _extract_and_save_section(
            client, storage_client, quarterly_filing, "risk", config.RISK_FOLDER
        )

    return f"{ticker}: SEC filing extraction and cleanup complete."


def run_pipeline(client: SecApiClient, storage_client: storage.Client):
    """Runs the full SEC filing extraction pipeline."""
    tickers = get_tickers(storage_client)
    if not tickers:
        logging.error("No tickers found. Exiting SEC pipeline.")
        return

    logging.info(
        f"Starting SEC extraction for {len(tickers)} tickers with cleanup logic."
    )
    max_workers = config.MAX_WORKERS_TIERING.get("sec_filing_extractor")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_ticker, t, client, storage_client): t
            for t in tickers
        }
        for future in as_completed(futures):
            try:
                logging.info(future.result())
            except Exception as e:
                logging.error(
                    f"'{futures[future]}': An error occurred during SEC processing: {e}",
                    exc_info=True,
                )
    logging.info("SEC filing extraction pipeline complete.")
