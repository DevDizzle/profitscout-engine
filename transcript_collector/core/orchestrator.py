import logging
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery, storage, pubsub_v1
from .. import config
from .gcs import list_existing_transcripts, upload_json_to_gcs
from .client import FMPClient

def get_required_transcripts_from_bq(bq_client: bigquery.Client) -> set:
    """
    Queries the master metadata table in BigQuery to get the full set of
    (ticker, quarter_end_date_str, year, quarter) tuples that should exist.
    """
    logging.info(f"Querying BigQuery table: {config.BIGQUERY_TABLE_ID}")
    # --- NEW: Query for the new year and quarter columns ---
    query = f"""
        SELECT ticker, quarter_end_date, earnings_year, earnings_quarter
        FROM `{config.BIGQUERY_TABLE_ID}`
        WHERE ticker IS NOT NULL 
          AND quarter_end_date IS NOT NULL
          AND earnings_year IS NOT NULL
          AND earnings_quarter IS NOT NULL
    """
    try:
        df = bq_client.query(query).to_dataframe()
        required_set = set()
        for index, row in df.iterrows():
            date_str = row['quarter_end_date'].strftime('%Y-%m-%d')
            # --- NEW: Create a tuple with all the needed info ---
            item = (
                row['ticker'], 
                date_str, 
                int(row['earnings_year']), 
                int(row['earnings_quarter'])
            )
            required_set.add(item)

        logging.info(f"Found {len(required_set)} required transcripts in BigQuery.")
        if required_set:
            logging.info(f"Sample of required transcripts: {list(required_set)[:5]}")
        return required_set
    except Exception as e:
        logging.critical(f"Failed to query BigQuery for required transcripts: {e}", exc_info=True)
        return set()

def process_missing_transcript(
    item: tuple, fmp_client: FMPClient, storage_client: storage.Client, publisher_client: pubsub_v1.PublisherClient
):
    """
    Fetches, uploads, and publishes a message for a single missing transcript.
    """
    # --- NEW: Unpack the year and quarter directly from the item tuple ---
    ticker, date_str, year, quarter = item

    logging.info(f"Processing missing transcript for {ticker} Q{quarter} {year}")

    try:
        # --- The rest of the logic is now correct because year/quarter are guaranteed to be right ---
        transcript_data = fmp_client.fetch_transcript(ticker, year, quarter)
        if not (transcript_data and transcript_data.get("content")):
            return f"NO_CONTENT: {ticker} for {year} Q{quarter}. API returned no data."

        blob_path = f"{config.GCS_OUTPUT_FOLDER}{ticker}_{date_str}.json"
        upload_json_to_gcs(storage_client, config.GCS_BUCKET_NAME, transcript_data, blob_path)

        topic_path = publisher_client.topic_path(config.PROJECT_ID, config.PUB_SUB_TOPIC_ID)
        message_data = {"gcs_path": f"gs://{config.GCS_BUCKET_NAME}/{blob_path}"}
        future = publisher_client.publish(topic_path, json.dumps(message_data).encode("utf-8"))
        future.result() 

        return f"SUCCESS: Fetched, uploaded, and published for {ticker} on {date_str}"

    except Exception as e:
        logging.error(f"ERROR: Failed to process {ticker} for {date_str}: {e}", exc_info=True)
        return f"ERROR: {ticker} on {date_str}"


def run_pipeline(fmp_client: FMPClient, bq_client: bigquery.Client, storage_client: storage.Client, publisher_client: pubsub_v1.PublisherClient):
    """Runs the full transcript collection pipeline using a diff-based approach."""
    required_set = get_required_transcripts_from_bq(bq_client)
    if not required_set:
        return

    existing_set = list_existing_transcripts(storage_client, config.GCS_BUCKET_NAME, config.GCS_OUTPUT_FOLDER)
    if existing_set:
        logging.info(f"Sample of existing transcripts in GCS: {list(existing_set)[:5]}")

    # --- NEW: Modify the diff logic to work with the new tuple structure ---
    # We only care about (ticker, date_str) for the diff
    required_for_diff = {(item[0], item[1]) for item in required_set}
    missing_filenames = required_for_diff - existing_set
    
    # Create a map to easily find the full item details for the missing files
    item_map = {(item[0], item[1]): item for item in required_set}
    missing_items = {item_map[fname] for fname in missing_filenames}

    if not missing_items:
        logging.info("Pipeline complete. No missing transcripts found.")
        return

    logging.info(f"Found {len(missing_items)} missing transcripts to process.")
    if missing_items:
        logging.info(f"Sample of missing items to fetch: {list(missing_items)[:5]}")

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_missing_transcript, item, fmp_client, storage_client, publisher_client): item 
            for item in missing_items
        }
        for future in as_completed(futures):
            try:
                logging.info(future.result())
            except Exception as e:
                logging.error(f"A future failed for item {futures[future]}: {e}", exc_info=True)
    logging.info("Transcript collection pipeline complete.")