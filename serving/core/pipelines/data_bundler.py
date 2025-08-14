# serving/core/pipelines/data_bundler.py
import logging
import json
import pandas as pd
from datetime import date
from typing import Any, Dict, List, Optional
from google.api_core import exceptions
from google.cloud import bigquery, storage # <-- Make sure 'storage' is imported
from .. import config

def _get_ticker_work_list() -> pd.DataFrame:
    """Gets the base metadata for the latest quarter for each ticker."""
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    query = f"""
        SELECT ticker, company_name, industry, sector, quarter_end_date,
               earnings_call_date, earnings_year, earnings_quarter
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY quarter_end_date DESC) as rn
            FROM `{config.BUNDLER_STOCK_METADATA_TABLE_ID}`
            WHERE ticker IS NOT NULL AND quarter_end_date IS NOT NULL
        ) WHERE rn = 1
    """
    try:
        df = client.query(query).to_dataframe()
        logging.info(f"Successfully fetched {len(df)} tickers for bundling.")
        return df
    except Exception as e:
        logging.error(f"Failed to fetch ticker work list: {e}", exc_info=True)
        return pd.DataFrame()

def _get_weighted_scores() -> pd.DataFrame:
    """Fetches the latest weighted_score for each ticker."""
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    query = f"SELECT ticker, weighted_score FROM `{config.BUNDLER_SCORES_TABLE_ID}` WHERE weighted_score IS NOT NULL"
    try:
        df = client.query(query).to_dataframe()
        logging.info(f"Successfully fetched {len(df)} weighted scores.")
        return df
    except Exception as e:
        logging.error(f"Failed to fetch weighted scores: {e}", exc_info=True)
        return pd.DataFrame()

def _blob_exists(path: str) -> bool:
    """Checks if a blob exists in the data bucket."""
    from .. import gcs
    bucket = gcs._client().bucket(config.GCS_BUCKET_NAME)
    # --- CORRECTED LINE ---
    # This now correctly uses the 'storage' library, not 'bigquery.storage'
    blob = storage.Blob(name=path, bucket=bucket)
    return blob.exists(gcs._client())

def _load_blob_as_string(path: str) -> Optional[str]:
    """Loads a file from GCS as a string."""
    from .. import gcs
    try:
        bucket = gcs._client().bucket(config.GCS_BUCKET_NAME)
        return bucket.blob(path).download_as_string().decode('utf-8', 'replace')
    except exceptions.NotFound:
        return None

def _assemble_final_metadata(work_list_df: pd.DataFrame, scores_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Joins metadata with scores and gathers GCS asset links."""
    if scores_df.empty:
        logging.warning("No scores found. Cannot proceed.")
        return []
        
    merged_df = pd.merge(work_list_df, scores_df, on="ticker", how="inner")
    logging.info(f"Merged metadata and scores for {len(merged_df)} tickers.")

    final_records = []
    for _, row in merged_df.iterrows():
        ticker = row["ticker"]
        date_str = row["quarter_end_date"].strftime('%Y-%m-%d')
        
        asset_paths = {
            "technicals": f"technicals/{ticker}_technicals.json", "ratios": f"ratios/{ticker}_{date_str}.json",
            "profile": f"sec-business/{ticker}_{date_str}.json", "news": f"headline-news/{ticker}_{date.today().strftime('%Y-%m-%d')}.json",
            "mda": f"sec-mda/{ticker}_{date_str}.json", "key_metrics": f"key-metrics/{ticker}_{date_str}.json",
            "financials": f"financial-statements/{ticker}_{date_str}.json", "earnings_transcript": f"earnings-call-transcripts/{ticker}_{date_str}.json",
            "recommendation_analysis": f"recommendations/{ticker}_recommendation.json"
        }

        if not all(_blob_exists(path) for path in asset_paths.values()):
            logging.warning(f"[{ticker}] Missing one or more data assets. Skipping.")
            continue

        record = row.to_dict()
        for key, path in asset_paths.items():
            record[key] = f"gs://{config.GCS_BUCKET_NAME}/{path}"
        
        try:
            rec_content = _load_blob_as_string(asset_paths["recommendation_analysis"])
            record["recommendation"] = json.loads(rec_content).get("recommendation", "HOLD")
        except: record["recommendation"] = "HOLD"
            
        final_records.append(record)
    return final_records

def run_pipeline():
    """Orchestrates the final assembly and loading of asset metadata."""
    logging.info("--- Starting Data Bundler (Final Assembly) Pipeline ---")
    work_list_df = _get_ticker_work_list()
    if work_list_df.empty:
        logging.warning("No tickers in work list. Shutting down.")
        return

    scores_df = _get_weighted_scores()
    final_metadata = _assemble_final_metadata(work_list_df, scores_df)
    
    if not final_metadata:
        logging.warning("No complete records to load to BigQuery.")
        return
    
    from .. import bq
    df = pd.DataFrame(final_metadata)
    bq.load_df_to_bq(df, config.BUNDLER_ASSET_METADATA_TABLE_ID, config.DESTINATION_PROJECT_ID, write_disposition="WRITE_TRUNCATE")
    logging.info("--- Data Bundler (Final Assembly) Pipeline Finished ---")