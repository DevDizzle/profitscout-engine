import json
import logging
import os
import pandas as pd
from datetime import date
from typing import Any, Dict, List, Optional

from google.api_core import exceptions
from google.cloud import bigquery, pubsub_v1, storage

# --- Configuration ---
SOURCE_PROJECT_ID = os.environ.get("PROJECT_ID")
DESTINATION_PROJECT_ID = os.environ.get("DESTINATION_PROJECT_ID")
LOCATION = os.environ.get("LOCATION", "US")
DATA_BUCKET_NAME = os.environ.get("DATA_BUCKET_NAME")
BUNDLE_BUCKET_NAME = os.environ.get("BUNDLE_BUCKET_NAME")
BQ_METADATA_TABLE = os.environ.get("BQ_METADATA_TABLE")
BQ_SOURCE_TABLE = os.environ.get("BQ_SOURCE_TABLE")
JOB_COMPLETE_TOPIC_NAME = os.environ.get("JOB_COMPLETE_TOPIC")
JOB_COMPLETE_TOPIC = f"projects/{SOURCE_PROJECT_ID}/topics/{JOB_COMPLETE_TOPIC_NAME}"

# --- Initialize Clients ---
storage_client = storage.Client()
bq_client = bigquery.Client(project=SOURCE_PROJECT_ID)
publisher = pubsub_v1.PublisherClient()

def get_ticker_work_list_from_bq() -> List[Dict[str, Any]]:
    """
    Queries BigQuery to get the full record for the latest quarter for each ticker.
    """
    logging.info(f"Fetching master work list from BigQuery table: {BQ_SOURCE_TABLE}")
    query = f"""
        SELECT
            ticker, company_name, industry, sector, quarter_end_date,
            earnings_call_date, earnings_year, earnings_quarter
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY quarter_end_date DESC) as rn
            FROM `{SOURCE_PROJECT_ID}.{BQ_SOURCE_TABLE}`
            WHERE ticker IS NOT NULL AND quarter_end_date IS NOT NULL
        )
        WHERE rn = 1
    """
    try:
        results = bq_client.query(query).to_dataframe()
        work_list = results.to_dict('records')
        logging.info(f"Successfully fetched {len(work_list)} tickers to process.")
        return work_list
    except Exception as e:
        logging.error(f"Failed to fetch ticker list from BigQuery: {e}")
        return []


def create_and_upload_bundle(work_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Main worker function. Builds a text file bundle for LLM parsing.
    Returns the metadata dictionary ONLY if the bundle is created successfully.
    """
    ticker = work_item.get("ticker")
    max_date = work_item.get("quarter_end_date")

    if not ticker or not max_date:
        logging.warning(f"Work item is missing ticker or quarter_end_date: {work_item}")
        return None

    logging.info(f"Processing ticker: {ticker} for date: {max_date}")
    date_str = max_date.strftime('%Y-%m-%d')
    
    text_bundle_parts = []
    text_bundle_parts.append(f"=== TICKER METADATA ===\n\n" \
                  f"Ticker: {ticker}\n" \
                  f"Bundle Creation Date: {date.today().isoformat()}\n\n")

    data_sources = {
        "earnings_call_summary": f"earnings-call-transcripts/{ticker}_{date_str}.json",
        "financial_statements": f"financial-statements/{ticker}_{date_str}.json",
        "key_metrics": f"key-metrics/{ticker}_{date_str}.json",
        "ratios": f"ratios/{ticker}_{date_str}.json",
        "technicals": f"technicals/{ticker}_technicals.json",
        "business_profile": f"sec-business/{ticker}_business_profile.json",
        "prices": f"prices/{ticker}_90_day_prices.json",
    }

    # --- Fail-Fast Logic ---
    for key, path in data_sources.items():
        content = _load_blob_as_string(path)
        if content is None:
            logging.warning(f"[{ticker}] Missing data for '{key}'. Skipping bundle creation.")
            return None
        text_bundle_parts.append(f"=== {key.upper().replace('_', ' ')} ===\n\n{content}\n\n")

    mda_content = _load_sec_mda(ticker, date_str)
    if mda_content is None:
        logging.warning(f"[{ticker}] Missing SEC MDA data. Skipping bundle creation.")
        return None
    text_bundle_parts.append(f"=== SEC MDA ===\n\n{mda_content}\n\n")

    text_bundle = "".join(text_bundle_parts)

    try:
        bundle_name = f"bundles/{ticker}_{date_str}_bundle.txt"
        dest_bucket = storage_client.bucket(BUNDLE_BUCKET_NAME)
        blob = dest_bucket.blob(bundle_name)
        blob.upload_from_string(data=text_bundle, content_type="text/plain")
        
        bundle_gcs_path = f"gs://{BUNDLE_BUCKET_NAME}/{bundle_name}"
        logging.info(f"[{ticker}] Successfully uploaded text bundle to {bundle_gcs_path}")

        final_record = work_item.copy()
        final_record["bundle_gcs_path"] = bundle_gcs_path
        return final_record

    except Exception as e:
        logging.error(f"[{ticker}] Failed to upload bundle: {e}")
        return None


def replace_asset_metadata_in_bq(metadata_list: List[Dict[str, Any]]):
    """
    Wipes the existing BigQuery metadata table and loads the fresh, complete data.
    """
    if not metadata_list:
        logging.warning("No successful bundles were created. The BigQuery table will be empty.")
    
    logging.info(f"Starting fresh load of {len(metadata_list)} records into BigQuery.")
    
    destination_table_id = f"{DESTINATION_PROJECT_ID}.{BQ_METADATA_TABLE}"

    job_config = bigquery.LoadJobConfig(
        # The schema should match your final table exactly
        schema=[
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("company_name", "STRING"),
            bigquery.SchemaField("industry", "STRING"),
            bigquery.SchemaField("sector", "STRING"),
            bigquery.SchemaField("quarter_end_date", "DATE"),
            bigquery.SchemaField("earnings_call_date", "DATE"),
            bigquery.SchemaField("earnings_year", "INTEGER"),
            bigquery.SchemaField("earnings_quarter", "INTEGER"),
            bigquery.SchemaField("bundle_gcs_path", "STRING"),
        ],
        # This is the key change: it wipes the table before writing new data
        write_disposition="WRITE_TRUNCATE",
    )
    
    # Convert date objects to ISO format strings for BigQuery compatibility
    for row in metadata_list:
        if isinstance(row.get('quarter_end_date'), date):
            row['quarter_end_date'] = row['quarter_end_date'].isoformat()
        if isinstance(row.get('earnings_call_date'), date):
            row['earnings_call_date'] = row['earnings_call_date'].isoformat()

    try:
        load_job = bq_client.load_table_from_json(metadata_list, destination_table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        logging.info(f"Successfully wiped and reloaded {load_job.output_rows} records to {destination_table_id}")
    except Exception as e:
        logging.error(f"An error occurred during the BigQuery table reload: {e}")
        raise


def publish_job_complete_message():
    """Publishes a single message to Pub/Sub to signal that the data is ready."""
    try:
        message_data = b"Data bundler job completed. The BigQuery table is ready for sync."
        future = publisher.publish(JOB_COMPLETE_TOPIC, message_data)
        future.result()
        logging.info(f"Successfully published job completion message to {JOB_COMPLETE_TOPIC}")
    except Exception as e:
        logging.error(f"Failed to publish completion message: {e}")


# --- Helper Functions (No Changes) ---

def _load_blob_as_string(gcs_path: str) -> Optional[str]:
    """Loads any file from GCS and returns its content as a raw string."""
    try:
        bucket = storage_client.bucket(DATA_BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        return blob.download_as_string().decode('utf-8', 'replace')
    except exceptions.NotFound:
        return None
    except Exception as e:
        logging.error(f"Failed to load GCS blob at '{gcs_path}': {e}")
        return None

def _load_sec_mda(ticker: str, date_str: str) -> Optional[str]:
    """Tries to load the 10-Q and then the 10-K for the given ticker and date."""
    path_10q = f"sec-mda/{ticker}_{date_str}_10-Q.json"
    content = _load_blob_as_string(path_10q)
    if content is not None:
        return content
    path_10k = f"sec-mda/{ticker}_{date_str}_10-K.json"
    return _load_blob_as_string(path_10k)