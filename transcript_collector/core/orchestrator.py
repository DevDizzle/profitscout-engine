# transcript_collector/core/orchestrator.py
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
    (ticker, quarter_end_date) tuples that should exist.
    """
    logging.info(f"Querying BigQuery table: {config.BIGQUERY_TABLE_ID}")
    query = f"""
        SELECT ticker, quarter_end_date
        FROM `{config.BIGQUERY_TABLE_ID}`
        WHERE ticker IS NOT NULL AND quarter_end_date IS NOT NULL
    """
    try:
        # .to_dataframe() is a convenient way to get results
        df = bq_client.query(query).to_dataframe()
        required_set = set()
        for index, row in df.iterrows():
            # Format date to string 'YYYY-MM-DD' to match GCS file names
            date_str = row['quarter_end_date'].strftime('%Y-%m-%d')
            required_set.add((row['ticker'], date_str))

        logging.info(f"Found {len(required_set)} required transcripts in BigQuery.")
        return required_set
    except Exception as e:
        logging.critical(f"Failed to query BigQuery for required transcripts: {e}")
        return set()

def process_missing_transcript(
    item: tuple, fmp_client: FMPClient, storage_client: storage.Client, publisher_client: pubsub_v1.PublisherClient
):
    """
    Fetches, uploads, and publishes a message for a single missing transcript.
    """
    ticker, date_str = item
    dt = pd.to_datetime(date_str)
    year, quarter = dt.year, dt.quarter

    logging.info(f"Processing missing transcript for {ticker} Q{quarter} {year}")

    try:
        transcript_data = fmp_client.fetch_transcript(ticker, year, quarter)
        if not (transcript_data and transcript_data.get("content")):
            return f"NO_CONTENT: {ticker} for {year} Q{quarter}. API returned no data."

        blob_path = f"{config.GCS_OUTPUT_FOLDER}{ticker}_{date_str}.json"

        # Upload the fetched data to GCS
        upload_json_to_gcs(storage_client, config.GCS_BUCKET_NAME, transcript_data, blob_path)

        # Publish a message to the next topic
        topic_path = publisher_client.topic_path(config.PROJECT_ID, config.PUB_SUB_TOPIC_ID)
        message_data = {"gcs_path": f"gs://{config.GCS_BUCKET_NAME}/{blob_path}"}
        future = publisher_client.publish(topic_path, json.dumps(message_data).encode("utf-8"))
        future.result() # Wait for publish to complete

        return f"SUCCESS: Fetched, uploaded, and published for {ticker} on {date_str}"

    except Exception as e:
        logging.error(f"ERROR: Failed to process {ticker} for {date_str}: {e}", exc_info=True)
        return f"ERROR: {ticker} on {date_str}"


def run_pipeline(fmp_client: FMPClient, bq_client: bigquery.Client, storage_client: storage.Client, publisher_client: pubsub_v1.PublisherClient):
    """Runs the full transcript collection pipeline using a diff-based approach."""

    # 1. Get the full list of required transcripts from our source of truth (BigQuery)
    required_set = get_required_transcripts_from_bq(bq_client)
    if not required_set:
        logging.error("Could not retrieve list of required transcripts from BigQuery. Exiting.")
        return

    # 2. Get the full list of transcripts that already exist in our destination (GCS)
    existing_set = list_existing_transcripts(storage_client, config.GCS_BUCKET_NAME, config.GCS_OUTPUT_FOLDER)

    # 3. Calculate the difference to find what work needs to be done
    missing_items = required_set - existing_set

    if not missing_items:
        logging.info("Pipeline complete. No missing transcripts found.")
        return

    logging.info(f"Found {len(missing_items)} missing transcripts to process.")

    # 4. Process all missing items in parallel
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_missing_transcript, item, fmp_client, storage_client, publisher_client): item 
            for item in missing_items
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                logging.info(result)
            except Exception as e:
                item = futures[future]
                logging.error(f"A future failed for item {item}: {e}", exc_info=True)

    logging.info("Transcript collection pipeline complete.")