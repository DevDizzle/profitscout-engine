# serving/functions/data_bundler/core/bundler.py

import json
import logging
import os
import pandas as pd
from datetime import date
from typing import Any, Dict, List, Optional

from google.api_core import exceptions
from google.cloud import bigquery, storage

# --- Configuration ---
SOURCE_PROJECT_ID = os.environ.get("PROJECT_ID") # e.g., profitscout-lx6bb
DESTINATION_PROJECT_ID = os.environ.get("DESTINATION_PROJECT_ID") # e.g., profitscout-fida8
DATA_BUCKET_NAME = os.environ.get("DATA_BUCKET_NAME")
BQ_METADATA_TABLE = os.environ.get("BQ_METADATA_TABLE") # The final destination table
BQ_SOURCE_TABLE = os.environ.get("BQ_SOURCE_TABLE") # The initial work list
BQ_SCORES_TABLE = os.environ.get("BQ_SCORES_TABLE") # The table with weighted_scores

# --- Initialize Clients ---
storage_client = storage.Client()
# This client will be used for reading from the source project
bq_source_client = bigquery.Client(project=SOURCE_PROJECT_ID)
# This client is specifically for writing to the destination project
bq_dest_client = bigquery.Client(project=DESTINATION_PROJECT_ID)


def get_ticker_work_list_from_bq() -> pd.DataFrame:
    """Gets the base metadata for the latest quarter for each ticker."""
    logging.info(f"Fetching master work list from: {BQ_SOURCE_TABLE}")
    query = f"""
        SELECT
            ticker, company_name, industry, sector, quarter_end_date,
            earnings_call_date, earnings_year, earnings_quarter
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY quarter_end_date DESC) as rn
            FROM `{BQ_SOURCE_TABLE}`
            WHERE ticker IS NOT NULL AND quarter_end_date IS NOT NULL
        )
        WHERE rn = 1
    """
    try:
        df = bq_source_client.query(query).to_dataframe()
        logging.info(f"Successfully fetched {len(df)} tickers to process.")
        return df
    except Exception as e:
        logging.error(f"Failed to fetch ticker list from BigQuery: {e}")
        return pd.DataFrame()

def get_weighted_scores_from_bq() -> pd.DataFrame:
    """Fetches the latest weighted_score for each ticker from the source project."""
    logging.info(f"Fetching weighted scores from: {BQ_SCORES_TABLE}")
    query = f"""
        SELECT ticker, weighted_score
        FROM `{BQ_SCORES_TABLE}`
        WHERE weighted_score IS NOT NULL
    """
    try:
        df = bq_source_client.query(query).to_dataframe()
        logging.info(f"Successfully fetched {len(df)} weighted scores.")
        return df
    except Exception as e:
        logging.error(f"Failed to fetch weighted scores from BigQuery: {e}")
        return pd.DataFrame()

def assemble_final_metadata(work_list_df: pd.DataFrame, scores_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Joins the work list with scores and gathers GCS asset links for each ticker.
    This replaces the previous 'gather_asset_links' function.
    """
    # Merge the base metadata with the calculated scores
    if scores_df.empty:
        logging.warning("No scores found. Cannot proceed with assembly.")
        return []
        
    merged_df = pd.merge(work_list_df, scores_df, on="ticker", how="inner")
    logging.info(f"Successfully merged metadata and scores. {len(merged_df)} tickers have a score.")

    final_records = []
    # Iterate over the merged dataframe to build the final records
    for _, row in merged_df.iterrows():
        ticker = row["ticker"]
        date_str = row["quarter_end_date"].strftime('%Y-%m-%d')
        
        asset_paths = {
            "technicals": f"technicals/{ticker}_technicals.json",
            "ratios": f"ratios/{ticker}_{date_str}.json",
            "profile": f"sec-business/{ticker}_{date_str}.json",
            "news": f"headline-news/{ticker}_{date.today().strftime('%Y-%m-%d')}.json",
            "mda": f"sec-mda/{ticker}_{date_str}.json",
            "key_metrics": f"key-metrics/{ticker}_{date_str}.json",
            "financials": f"financial-statements/{ticker}_{date_str}.json",
            "earnings_transcript": f"earnings-call-transcripts/{ticker}_{date_str}.json",
            "recommendation_analysis": f"recommendations/{ticker}_recommendation.json"
        }

        # Check for existence of all files. If any are missing, skip this ticker.
        if not all(_blob_exists(path) for path in asset_paths.values()):
            logging.warning(f"[{ticker}] Missing one or more data assets. Skipping.")
            continue

        # Build the final record for BigQuery
        record = row.to_dict()
        for key, path in asset_paths.items():
            record[key] = f"gs://{DATA_BUCKET_NAME}/{path}"
        
        # Extract the recommendation from the JSON file
        try:
            rec_content = _load_blob_as_string(asset_paths["recommendation_analysis"])
            record["recommendation"] = json.loads(rec_content).get("recommendation", "HOLD")
        except Exception:
            record["recommendation"] = "HOLD"
            
        final_records.append(record)
        
    return final_records


def replace_asset_metadata_in_bq(metadata_list: List[Dict[str, Any]]):
    """
    Wipes the destination BigQuery table and loads the final, assembled data.
    """
    if not metadata_list:
        logging.warning("No complete asset sets were found. The destination BigQuery table will not be updated.")
        return

    destination_table_id = f"{DESTINATION_PROJECT_ID}.{BQ_METADATA_TABLE}"
    logging.info(f"Starting fresh load of {len(metadata_list)} records to: {destination_table_id}")

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
            bigquery.SchemaField("weighted_score", "FLOAT"), 
            bigquery.SchemaField("technicals", "STRING"),
            bigquery.SchemaField("ratios", "STRING"),
            bigquery.SchemaField("profile", "STRING"),
            bigquery.SchemaField("news", "STRING"),
            bigquery.SchemaField("mda", "STRING"),
            bigquery.SchemaField("key_metrics", "STRING"),
            bigquery.SchemaField("financials", "STRING"),
            bigquery.SchemaField("earnings_transcript", "STRING"),
            bigquery.SchemaField("recommendation", "STRING"),
            bigquery.SchemaField("recommendation_analysis", "STRING"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    df = pd.DataFrame(metadata_list)
    # Convert date objects to strings for BQ compatibility before loading
    for col in df.select_dtypes(include=['object']).columns:
        if isinstance(df[col].iloc[0], date):
             df[col] = pd.to_datetime(df[col]).dt.date.astype(str)

    try:
        # Use the destination client to load the data
        load_job = bq_dest_client.load_table_from_dataframe(df, destination_table_id, job_config=job_config)
        load_job.result()
        logging.info(f"Successfully loaded {load_job.output_rows} records to {destination_table_id}")
    except Exception as e:
        logging.error(f"BigQuery load job to destination project failed: {e}", exc_info=True)
        raise
        
# Helper functions _blob_exists and _load_blob_as_string remain the same
def _blob_exists(gcs_path: str) -> bool:
    """Checks if a blob exists in the data bucket without downloading it."""
    try:
        bucket = storage_client.bucket(DATA_BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        return blob.exists()
    except Exception as e:
        logging.error(f"Failed to check existence for GCS blob at '{gcs_path}': {e}")
        return False

def _load_blob_as_string(gcs_path: str) -> Optional[str]:
    """Loads a file from GCS and returns its content as a raw string."""
    try:
        bucket = storage_client.bucket(DATA_BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        return blob.download_as_string().decode('utf-8', 'replace')
    except exceptions.NotFound:
        return None
    except Exception as e:
        logging.error(f"Failed to load GCS blob at '{gcs_path}': {e}")
        return None