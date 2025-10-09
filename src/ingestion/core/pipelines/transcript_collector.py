# ingestion/core/orchestrators/transcript_collector.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery, storage
from .. import config
from ..gcs import list_existing_transcripts, upload_json_to_gcs
from ..clients.fmp_client import FMPClient

def _get_required_transcripts_from_bq(bq_client: bigquery.Client) -> set:
    """Queries BigQuery to get the set of (ticker, date_str, year, quarter) tuples."""
    logging.info(f"Querying BigQuery table: {config.MASTER_TABLE_ID}")
    query = f"""
        SELECT ticker, quarter_end_date, earnings_year, earnings_quarter
        FROM `{config.MASTER_TABLE_ID}`
        WHERE ticker IS NOT NULL AND quarter_end_date IS NOT NULL
          AND earnings_year IS NOT NULL AND earnings_quarter IS NOT NULL
    """
    try:
        df = bq_client.query(query).to_dataframe()
        required_set = {
            (row['ticker'], row['quarter_end_date'].strftime('%Y-%m-%d'), int(row['earnings_year']), int(row['earnings_quarter']))
            for index, row in df.iterrows()
        }
        logging.info(f"Found {len(required_set)} required transcripts in BigQuery.")
        return required_set
    except Exception as e:
        logging.critical(f"Failed to query BigQuery for required transcripts: {e}", exc_info=True)
        return set()

def _process_missing_transcript(item: tuple, fmp_client: FMPClient, storage_client: storage.Client):
    """Fetches and uploads a single missing transcript."""
    ticker, date_str, year, quarter = item
    logging.info(f"Processing missing transcript for {ticker} Q{quarter} {year}")

    try:
        transcript_data = fmp_client.fetch_transcript(ticker, year, quarter)
        if not (transcript_data and transcript_data.get("content")):
            return f"NO_CONTENT: {ticker} for {year} Q{quarter}."

        blob_path = f"{config.TRANSCRIPT_OUTPUT_FOLDER}{ticker}_{date_str}.json"
        upload_json_to_gcs(storage_client, transcript_data, blob_path)
        return f"SUCCESS: Fetched and uploaded for {ticker} on {date_str}"
    except Exception as e:
        return f"ERROR processing {ticker} for {date_str}: {e}"

def run_pipeline(fmp_client: FMPClient, bq_client: bigquery.Client, storage_client: storage.Client):
    """Runs the full transcript collection pipeline using a diff-based approach."""
    required_set = _get_required_transcripts_from_bq(bq_client)
    if not required_set: return

    existing_set = list_existing_transcripts(storage_client)
    
    required_for_diff = {(item[0], item[1]) for item in required_set}
    missing_filenames = required_for_diff - existing_set
    
    item_map = {(item[0], item[1]): item for item in required_set}
    missing_items = {item_map[fname] for fname in missing_filenames}

    if not missing_items:
        logging.info("Pipeline complete. No missing transcripts found.")
        return

    logging.info(f"Found {len(missing_items)} missing transcripts to process.")
    max_workers = config.MAX_WORKERS_TIERING.get("transcript_collector")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_missing_transcript, item, fmp_client, storage_client): item for item in missing_items}
        for future in as_completed(futures):
            try:
                logging.info(future.result())
            except Exception as e:
                logging.error(f"A future failed for item {futures[future]}: {e}", exc_info=True)
    logging.info("Transcript collection pipeline complete.")