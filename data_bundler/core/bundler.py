import json
import logging
import os
import pandas as pd
from datetime import date
from typing import Any, Dict, List, Optional

from google.api_core import exceptions
from google.cloud import bigquery, pubsub_v1, storage

# --- Configuration from Environment Variables ---
SOURCE_PROJECT_ID = os.environ.get("PROJECT_ID")
DESTINATION_PROJECT_ID = os.environ.get("DESTINATION_PROJECT_ID")

LOCATION = os.environ.get("LOCATION", "US")
DATA_BUCKET_NAME = os.environ.get("DATA_BUCKET_NAME")
BUNDLE_BUCKET_NAME = os.environ.get("BUNDLE_BUCKET_NAME")
BQ_METADATA_TABLE = os.environ.get("BQ_METADATA_TABLE")
BQ_SOURCE_TABLE = os.environ.get("BQ_SOURCE_TABLE")
JOB_COMPLETE_TOPIC_NAME = os.environ.get("JOB_COMPLETE_TOPIC")
JOB_COMPLETE_TOPIC = f"projects/{SOURCE_PROJECT_ID}/topics/{JOB_COMPLETE_TOPIC_NAME}"


# Initialize clients
storage_client = storage.Client()
bq_client = bigquery.Client(project=SOURCE_PROJECT_ID)
publisher = pubsub_v1.PublisherClient()

# --- Main Functions ---

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
    Main worker function. Passes all metadata through to the final dictionary.
    Builds a text file bundle with clear section headings for better LLM parsing.
    """
    ticker = work_item.get("ticker")
    max_date = work_item.get("quarter_end_date")

    if not ticker or not max_date:
        logging.warning(f"Work item is missing ticker or quarter_end_date: {work_item}")
        return None

    logging.info(f"Processing ticker: {ticker} for date: {max_date}")

    date_str = max_date.strftime('%Y-%m-%d')
    text_bundle = f"=== TICKER METADATA ===\n\n" \
                  f"Ticker: {ticker}\n" \
                  f"Bundle Creation Date: {date.today().isoformat()}\n\n"

    data_sources = {
        "earnings_call_summary": f"earnings-call-transcripts/{ticker}_{date_str}.json",
        "financial_statements": f"financial-statements/{ticker}_{date_str}.json",
        "key_metrics": f"key-metrics/{ticker}_{date_str}.json",
        "ratios": f"ratios/{ticker}_{date_str}.json",
        "technicals": f"technicals/{ticker}_technicals.json",
        "business_profile": f"sec-business/{ticker}_business_profile.json",
        "prices": f"prices/{ticker}_90_day_prices.json",
    }

    for key, path in data_sources.items():
        content = _load_blob_as_string(path)
        if content is not None:
            # If content looks like JSON, indent it for readability in text
            try:
                json_content = json.loads(content)
                content = json.dumps(json_content, indent=4)  # Pretty-print JSON as text
            except json.JSONDecodeError:
                pass  # Not JSON; leave as-is
            text_bundle += f"=== {key.upper().replace('_', ' ')} ===\n\n{content}\n\n"
        else:
            logging.warning(f"[{ticker}] Missing data for '{key}' at path: {path}")
            text_bundle += f"=== {key.upper().replace('_', ' ')} ===\n\n[No data available]\n\n"

    mda_content = _load_sec_mda(ticker, date_str)
    if mda_content is not None:
        try:
            json_content = json.loads(mda_content)
            mda_content = json.dumps(json_content, indent=4)
        except json.JSONDecodeError:
            pass
        text_bundle += f"=== SEC MDA ===\n\n{mda_content}\n\n"
    else:
        text_bundle += f"=== SEC MDA ===\n\n[No data available]\n\n"

    if len(text_bundle.splitlines()) <= 3:  # Rough check for empty-ish bundle
        logging.info(f"[{ticker}] No source data found. Skipping bundle creation.")
        return None

    try:
        bundle_name = f"bundles/{ticker}_{date_str}_bundle.txt"
        dest_bucket = storage_client.bucket(BUNDLE_BUCKET_NAME)
        blob = dest_bucket.blob(bundle_name)
        blob.upload_from_string(
            data=text_bundle,
            content_type="text/plain",
        )
        bundle_gcs_path = f"gs://{BUNDLE_BUCKET_NAME}/{bundle_name}"
        logging.info(f"[{ticker}] Successfully uploaded text bundle to {bundle_gcs_path}")

        final_record = work_item.copy()
        final_record["bundle_gcs_path"] = bundle_gcs_path
        return final_record

    except Exception as e:
        logging.error(f"[{ticker}] Failed to upload bundle: {e}")
        return None


def batch_update_asset_metadata_in_bq(metadata_list: List[Dict[str, Any]]):
    """
    Updates the BigQuery asset_metadata table, ensuring only one record per ticker.
    """
    if not metadata_list:
        logging.info("No metadata to update in BigQuery. Skipping.")
        return

    # --- FINAL FIX: De-duplicate the data before sending to BigQuery ---
    # Convert to DataFrame to easily drop duplicates, keeping the last entry for each ticker
    df = pd.DataFrame(metadata_list)
    df.drop_duplicates(subset=['ticker'], keep='last', inplace=True)
    # Convert back to a list of dictionaries for BigQuery
    deduplicated_list = df.to_dict('records')
    
    logging.info(f"Starting batch update of {len(deduplicated_list)} unique records in BigQuery.")

    temp_table_id = f"{DESTINATION_PROJECT_ID}.{BQ_METADATA_TABLE}_staging_{os.urandom(4).hex()}"
    destination_table_ref = f"`{DESTINATION_PROJECT_ID}.{BQ_METADATA_TABLE}`"

    try:
        job_config = bigquery.LoadJobConfig(
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
            write_disposition="WRITE_TRUNCATE",
        )

        for row in deduplicated_list:
            if isinstance(row.get('quarter_end_date'), date):
                row['quarter_end_date'] = row['quarter_end_date'].isoformat()
            if isinstance(row.get('earnings_call_date'), date):
                row['earnings_call_date'] = row['earnings_call_date'].isoformat()

        load_job = bq_client.load_table_from_json(deduplicated_list, temp_table_id, job_config=job_config)
        load_job.result()
        logging.info(f"Staged {len(deduplicated_list)} records to temporary table {temp_table_id}")

        merge_query = f"""
            MERGE {destination_table_ref} AS T
            USING `{temp_table_id}` AS S
            ON T.ticker = S.ticker
            WHEN MATCHED AND S.quarter_end_date >= T.quarter_end_date THEN
              UPDATE SET
                company_name = S.company_name,
                industry = S.industry,
                sector = S.sector,
                quarter_end_date = S.quarter_end_date,
                earnings_call_date = S.earnings_call_date,
                earnings_year = S.earnings_year,
                earnings_quarter = S.earnings_quarter,
                bundle_gcs_path = S.bundle_gcs_path,
                last_updated = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
              INSERT (
                  ticker, company_name, industry, sector, quarter_end_date,
                  earnings_call_date, earnings_year, earnings_quarter, bundle_gcs_path,
                  created_at, last_updated
              )
              VALUES(
                  S.ticker, S.company_name, S.industry, S.sector, S.quarter_end_date,
                  S.earnings_call_date, S.earnings_year, S.earnings_quarter, S.bundle_gcs_path,
                  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
              )
        """
        merge_job = bq_client.query(merge_query)
        merge_job.result()
        logging.info("Successfully merged data into the main asset_metadata table.")

    except Exception as e:
        logging.error(f"An error occurred during the BigQuery batch update: {e}")
        raise
    finally:
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        logging.info(f"Cleaned up temporary table: {temp_table_id}")


def publish_job_complete_message():
    """Publishes a single message to Pub/Sub to signal job completion."""
    try:
        message_data = b"Data bundler job completed successfully."
        future = publisher.publish(JOB_COMPLETE_TOPIC, message_data)
        future.result()
        logging.info(f"Successfully published completion message to {JOB_COMPLETE_TOPIC}")
    except Exception as e:
        logging.error(f"Failed to publish completion message: {e}")


# --- Helper Functions ---

def _load_blob_as_string(gcs_path: str) -> Optional[str]:
    """
    Loads any file from GCS and returns its content as a cleaned raw string.
    """
    try:
        bucket = storage_client.bucket(DATA_BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        content = blob.download_as_string().decode('utf-8', 'replace')
        # Cleaning: Remove non-printable chars, excess newlines, and trim
        content = ''.join(c for c in content if c.isprintable() or c in '\n\t\r')
        content = '\n'.join(line.strip() for line in content.splitlines() if line.strip())
        return content if content else None
    except exceptions.NotFound:
        return None
    except Exception as e:
        logging.error(f"Failed to load GCS blob at '{gcs_path}': {e}")
        return None

def _load_sec_mda(ticker: str, date_str: str) -> Optional[str]:
    """
    Tries to load the 10-Q and then the 10-K for the given ticker and date.
    """
    path_10q = f"sec-mda/{ticker}_{date_str}_10-Q.json"
    content = _load_blob_as_string(path_10q)
    if content is not None:
        return content

    path_10k = f"sec-mda/{ticker}_{date_str}_10-K.json"
    content = _load_blob_as_string(path_10k)
    if content is None:
        logging.warning(f"[{ticker}] No MD&A found (tried 10-Q and 10-K).")

    return content