# ingestion/core/pipelines/transcript_collector.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery, storage

from .. import config
from ..clients.fmp_client import FMPClient
from ..gcs import blob_exists, get_tickers, upload_json_to_gcs


def _process_latest_transcript(
    ticker: str, fmp_client: FMPClient, storage_client: storage.Client
):
    """Fetches the latest transcript and uploads ONLY if it's new."""
    try:
        # 1. Fetch latest transcript (API Call)
        # We accept the cost of this call to get the authoritative 'latest date'
        transcript = fmp_client.get_latest_transcript(ticker)

        if not transcript:
            return f"SKIPPED: No transcript found for {ticker}"

        # 2. Identify the file path
        # Extract date: YYYY-MM-DD (FMP date is usually '2024-03-31 16:00:00')
        date_str = transcript.get("date", "UNKNOWN").split(" ")[0]
        blob_name = f"{ticker}_{date_str}.json"
        blob_path = f"{config.TRANSCRIPT_OUTPUT_FOLDER}{blob_name}"

        # 3. CHECK EXISTENCE (The Logic from Financials/MD&A)
        # If we already have this exact date, do not overwrite.
        if blob_exists(storage_client, blob_path):
            return f"SKIPPED: {ticker} transcript for {date_str} already exists."

        # 4. Upload if new
        upload_json_to_gcs(storage_client, transcript, blob_path)
        return f"SUCCESS: Uploaded new transcript for {ticker} -> {blob_path}"

    except Exception as e:
        return f"ERROR: {ticker}: {str(e)}"


def run_pipeline(
    fmp_client: FMPClient, bq_client: bigquery.Client, storage_client: storage.Client
):
    """
    Main entry point: Read Tickers -> Fetch Latest -> Check Existence -> Upload.
    """
    # 1. Get Tickers from GCS
    tickers = get_tickers(storage_client)
    if not tickers:
        logging.warning("No tickers found in tickerlist.txt. Exiting.")
        return

    logging.info(f"Starting transcript collection for {len(tickers)} tickers.")

    # 2. Process in Parallel
    max_workers = config.MAX_WORKERS_TIERING.get("transcript_collector", 6)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_latest_transcript, ticker, fmp_client, storage_client
            ): ticker
            for ticker in tickers
        }

        for future in as_completed(futures):
            logging.info(future.result())

    logging.info("Transcript collection pipeline complete.")
