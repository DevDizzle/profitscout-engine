# serving/core/pipelines/data_bundler.py
import logging
import pandas as pd
from datetime import date
from typing import Any, Dict, List
from google.cloud import bigquery, storage
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, bq


def _delete_blob(blob, bucket):
    """Worker function to delete a single blob."""
    try:
        blob.delete()
        logging.info(f"Deleted {blob.name}")
        return blob.name
    except Exception as e:
        logging.error(f"Failed to delete {blob.name}: {e}")
        return None


def _copy_blob(blob, source_bucket, destination_bucket):
    """Worker function to copy a single blob."""
    try:
        source_blob = source_bucket.blob(blob.name)
        destination_blob = destination_bucket.blob(blob.name)

        token, bytes_rewritten, total_bytes = destination_blob.rewrite(source_blob)
        while token is not None:
            token, bytes_rewritten, total_bytes = destination_blob.rewrite(source_blob, token=token)

        logging.info(f"Successfully copied {blob.name}")
        return blob.name
    except Exception as e:
        logging.error(f"Failed to copy {blob.name}: {e}", exc_info=True)
        return None


def _sync_recommendations():
    """
    Deletes all blobs in the destination recommendations folder and then copies
    all recommendation files from the source in parallel.
    """
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(config.GCS_BUCKET_NAME, user_project=config.SOURCE_PROJECT_ID)
    destination_bucket = storage_client.bucket(
        config.DESTINATION_GCS_BUCKET_NAME, user_project=config.DESTINATION_PROJECT_ID
    )
    recommendation_prefix = config.RECOMMENDATION_PREFIX

    # --- Step 1: Delete all blobs in the destination recommendations folder ---
    logging.info(f"Starting deletion of all files in gs://{destination_bucket.name}/{recommendation_prefix}")
    blobs_to_delete = list(destination_bucket.list_blobs(prefix=recommendation_prefix))
    if not blobs_to_delete:
        logging.info("Destination recommendations folder is already empty. No files to delete.")
    else:
        deleted_count = 0
        with ThreadPoolExecutor(max_workers=config.MAX_WORKERS_BUNDLER) as executor:
            future_to_blob = {executor.submit(_delete_blob, blob, destination_bucket): blob for blob in blobs_to_delete}
            for future in as_completed(future_to_blob):
                if future.result():
                    deleted_count += 1
        logging.info(f"Deletion complete. Removed {deleted_count} of {len(blobs_to_delete)} files.")

    # --- Step 2: Copy all recommendation files from the source bucket ---
    logging.info(f"Starting parallel copy from gs://{source_bucket.name}/{recommendation_prefix} to gs://{destination_bucket.name}/{recommendation_prefix}")
    blobs_to_copy = list(source_bucket.list_blobs(prefix=recommendation_prefix))
    copied_count = 0

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS_BUNDLER) as executor:
        future_to_blob = {
            executor.submit(_copy_blob, blob, source_bucket, destination_bucket): blob
            for blob in blobs_to_copy
        }
        for future in as_completed(future_to_blob):
            if future.result():
                copied_count += 1

    logging.info(f"GCS recommendation file sync finished. Copied {copied_count} of {len(blobs_to_copy)} files.")


def _get_ticker_work_list() -> pd.DataFrame:
    """Gets the base metadata for the latest quarter for each ticker."""
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    query = f"""
        SELECT ticker, company_name, industry, sector, quarter_end_date,
               earnings_call_date, earnings_year, earnings_quarter
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY quarter_end_date DESC) as rn
            FROM {config.BUNDLER_STOCK_METADATA_TABLE_ID}
            WHERE ticker IS NOT NULL AND quarter_end_date IS NOT NULL
        ) WHERE rn = 1
    """
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        logging.error(f"Failed to fetch ticker work list: {e}", exc_info=True)
        return pd.DataFrame()


def _get_weighted_scores() -> pd.DataFrame:
    """Fetches the latest weighted_score for each ticker."""
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    query = f"SELECT ticker, weighted_score FROM {config.BUNDLER_SCORES_TABLE_ID} WHERE weighted_score IS NOT NULL"
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        logging.error(f"Failed to fetch weighted scores: {e}", exc_info=True)
        return pd.DataFrame()


def _assemble_final_metadata(work_list_df: pd.DataFrame, scores_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Joins metadata, adds GCS asset URIs, and generates a recommendation."""
    if scores_df.empty:
        return []

    merged_df = pd.merge(work_list_df, scores_df, on="ticker", how="inner")

    final_records = []
    today_str = date.today().strftime('%Y-%m-%d')
    for _, row in merged_df.iterrows():
        ticker = row["ticker"]
        date_str = row["quarter_end_date"].strftime('%Y-%m-%d')

        # Define all potential asset paths
        asset_paths = {
            "technicals": f"technicals/{ticker}_technicals.json",
            "ratios": f"ratios/{ticker}_{date_str}.json",
            "profile": f"sec-business/{ticker}_{date_str}.json",
            "news": f"headline-news/{ticker}_{today_str}.json",
            "mda": f"sec-mda/{ticker}_{date_str}.json",
            "key_metrics": f"key-metrics/{ticker}_{date_str}.json",
            "financials": f"financial-statements/{ticker}_{date_str}.json",
            "earnings_transcript": f"earnings-call-transcripts/{ticker}_{date_str}.json",
            "recommendation_analysis": f"recommendations/{ticker}_recommendation_{today_str}.md",
            # --- CHANGE IS HERE ---
            # Add the new column for the page generator's JSON output
            "page_json_uri": f"pages/{ticker}_page_{today_str}.json",
        }

        record = row.to_dict()

        # Add the GCS URI for each asset
        for key, path in asset_paths.items():
            record[key] = f"gs://{config.DESTINATION_GCS_BUCKET_NAME}/{path}"

        # Generate BUY/SELL/HOLD recommendation based on the weighted score
        weighted_score = row["weighted_score"]
        if weighted_score > 0.68:
            recommendation = "BUY"
        elif weighted_score >= 0.50:
            recommendation = "HOLD"
        else:
            recommendation = "SELL"
        record["recommendation"] = recommendation

        final_records.append(record)

    return final_records


def run_pipeline():
    """Orchestrates the final assembly and loading of asset metadata."""
    logging.info("--- Starting Data Bundler (Final Assembly) Pipeline ---")

    # First, sync only the recommendation files
    _sync_recommendations()

    work_list_df = _get_ticker_work_list()
    if work_list_df.empty:
        logging.warning("No tickers in work list. Shutting down.")
        return

    scores_df = _get_weighted_scores()
    final_metadata = _assemble_final_metadata(work_list_df, scores_df)

    if not final_metadata:
        logging.warning("No complete records to load to BigQuery.")
        return

    df = pd.DataFrame(final_metadata)
    bq.load_df_to_bq(df, config.BUNDLER_ASSET_METADATA_TABLE_ID, config.DESTINATION_PROJECT_ID, "WRITE_APPEND")
    logging.info("--- Data Bundler (Final Assembly) Pipeline Finished ---")