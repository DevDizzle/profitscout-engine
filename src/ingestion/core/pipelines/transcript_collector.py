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
    """Fetches the latest transcript, cleans up old versions, and uploads the new one."""
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

        # 3. Check for existing files (Cleanup Logic)
        bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
        # Prefix e.g. "earnings-call-transcripts/AAL_"
        prefix = f"{config.TRANSCRIPT_OUTPUT_FOLDER}{ticker}_"
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        should_upload = True
        
        for blob in blobs:
            # Check for exact match first
            if blob.name == blob_path:
                return f"SKIPPED: {ticker} transcript for {date_str} already exists."
            
            # Check for old/stale files
            # Filename format: TICKER_YYYY-MM-DD.json
            try:
                filename = blob.name.split("/")[-1]
                # Extract date part by removing prefix and extension
                # Note: tickers can have hyphens, so we replace carefully
                # Ideally, we split by '_' if we enforced that separator, but ticker might be "BRK-B"
                # Safer: remove ".json" and take last 10 chars if it's YYYY-MM-DD
                
                # Robust extraction: remove ticker prefix?
                # filename is e.g. "AAL_2025-10-23.json"
                # ticker is "AAL"
                if filename.startswith(f"{ticker}_") and filename.endswith(".json"):
                    existing_date_str = filename[len(ticker)+1:-5]
                    
                    # Compare dates (YYYY-MM-DD string comparison is valid)
                    if date_str > existing_date_str:
                        logging.info(f"Deleting stale transcript: {blob.name} (Newer available: {date_str})")
                        blob.delete()
                    elif date_str < existing_date_str:
                        # Existing file is newer than what we just fetched.
                        logging.warning(f"Found newer local transcript {existing_date_str} vs fetched {date_str}. Skipping upload.")
                        should_upload = False
            except Exception as e:
                logging.warning(f"Error parsing existing blob {blob.name}: {e}")
                # Continue safely
        
        if not should_upload:
            return f"SKIPPED: Newer transcript exists for {ticker}."

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
