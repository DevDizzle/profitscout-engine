# serving/core/pipelines/data_bundler.py
import json
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd
from google.cloud import bigquery, storage

from .. import bq, config, gcs


def _delete_gcs_prefix(bucket: storage.Bucket, prefix: str):
    """
    Deletes all blobs under a given prefix in a GCS bucket.
    """
    try:
        blobs_to_delete = list(bucket.list_blobs(prefix=prefix))
        if not blobs_to_delete:
            logging.info(
                f"No blobs found to delete in prefix: gs://{bucket.name}/{prefix}"
            )
            return

        logging.info(
            f"Deleting {len(blobs_to_delete)} blobs from gs://{bucket.name}/{prefix}"
        )
        # Use bucket.delete_blobs for efficient batch deletion
        for blob in blobs_to_delete:
            blob.delete()
        logging.info(
            f"Successfully deleted blobs from prefix: gs://{bucket.name}/{prefix}"
        )
    except Exception as e:
        logging.error(f"Failed to delete blobs in prefix {prefix}: {e}", exc_info=True)
        # Halt the process if deletion fails to prevent stale data.
        raise


def _copy_blob(blob, source_bucket, destination_bucket):
    """
    Worker function to copy a single blob. 'overwrite' is always true in this workflow.
    """
    try:
        source_blob = source_bucket.blob(blob.name)
        destination_blob = destination_bucket.blob(blob.name)

        token, _, _ = destination_blob.rewrite(source_blob)
        while token is not None:
            token, _, _ = destination_blob.rewrite(source_blob, token=token)

        logging.info(f"Successfully copied {blob.name}")
        return blob.name
    except Exception as e:
        logging.error(f"Failed to copy {blob.name}: {e}", exc_info=True)
        return None


def _sync_gcs_data():
    """
    Performs a full wipe-and-replace sync for all necessary GCS folders.
    This erases all old data in the destination prefixes before copying fresh data.
    """
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(
        config.GCS_BUCKET_NAME, user_project=config.SOURCE_PROJECT_ID
    )
    destination_bucket = storage_client.bucket(
        config.DESTINATION_GCS_BUCKET_NAME, user_project=config.DESTINATION_PROJECT_ID
    )

    # A single, comprehensive list of all folders to be completely refreshed daily.
    all_prefixes_to_sync = [
        "dashboards/",
        "images/",
        "news-analysis/",
        "pages/",
        "price-chart-json/",
        "price-chart-images/",
        "recommendations/",
        "macro-thesis/",
        "mda-analysis/",
        "transcript-analysis/",
        "business-summaries/",
        "fundamentals-analysis/",
    ]

    logging.info("--- Starting FULL Wipe-and-Replace GCS Sync ---")

    # Step 1: Wipe ALL destination folders for a clean copy.
    logging.info(f"Wiping {len(all_prefixes_to_sync)} destination prefixes...")
    with ThreadPoolExecutor(
        max_workers=config.MAX_WORKERS_BUNDLER, thread_name_prefix="Deleter"
    ) as executor:
        delete_futures = [
            executor.submit(_delete_gcs_prefix, destination_bucket, prefix)
            for prefix in all_prefixes_to_sync
        ]
        for future in as_completed(delete_futures):
            try:
                future.result()  # Wait for deletions to complete.
            except Exception as e:
                logging.critical(
                    f"A critical error occurred during GCS prefix deletion, halting sync: {e}",
                    exc_info=True,
                )
                # Stop the entire process if we can't guarantee a clean slate.
                raise RuntimeError(
                    "GCS prefix deletion failed, aborting sync to prevent data inconsistency."
                ) from e

    logging.info("Wipe complete. Starting full file copy process...")

    # Step 2: List all source blobs and copy them.
    copied_count = 0
    with ThreadPoolExecutor(
        max_workers=config.MAX_WORKERS_BUNDLER, thread_name_prefix="Copier"
    ) as executor:
        all_blobs_to_copy = [
            blob
            for prefix in all_prefixes_to_sync
            for blob in source_bucket.list_blobs(prefix=prefix)
        ]
        logging.info(f"Found {len(all_blobs_to_copy)} total files to copy.")

        future_to_blob = {
            executor.submit(_copy_blob, blob, source_bucket, destination_bucket): blob
            for blob in all_blobs_to_copy
        }

        for future in as_completed(future_to_blob):
            if future.result():
                copied_count += 1

    logging.info(f"GCS full sync finished. Copied {copied_count} files.")


def _get_latest_daily_files_map() -> dict[str, dict[str, str]]:
    """Lists daily files from GCS once and creates a map of the latest file URI for each ticker."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(
        config.DESTINATION_GCS_BUCKET_NAME, user_project=config.DESTINATION_PROJECT_ID
    )
    daily_prefixes = {
        "news": "headline-news/",
        "recommendation_analysis": "recommendations/",
        "pages_json": "pages/",
        "dashboard_json": "dashboards/",
        "price_chart_image_uri": "price-chart-images/",  # Add the new image folder
    }
    latest_files = defaultdict(dict)

    for key, prefix in daily_prefixes.items():
        blobs = bucket.list_blobs(prefix=prefix)
        ticker_files = defaultdict(list)
        for blob in blobs:
            try:
                ticker = blob.name.split("/")[-1].split("_")[0]
                ticker_files[ticker].append(blob.name)
            except IndexError:
                continue

        for ticker, names in ticker_files.items():
            latest_name = max(names)
            latest_files[ticker][
                key
            ] = f"gs://{config.DESTINATION_GCS_BUCKET_NAME}/{latest_name}"

    return latest_files


def _get_latest_kpis() -> dict[str, dict[str, Any]]:
    """Reads all recent prep files to get the latest price and 30-day change for each ticker."""
    blobs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix="prep/")
    latest_kpis = {}

    for blob_name in blobs:
        try:
            content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
            if not content:
                continue
            data = json.loads(content)
            ticker = data.get("ticker")
            if not ticker:
                continue

            if ticker not in latest_kpis or data.get("runDate") > latest_kpis[
                ticker
            ].get("runDate"):
                kpis = data.get("kpis", {})
                latest_kpis[ticker] = {
                    "price": kpis.get("trendStrength", {}).get("price"),
                    "thirty_day_change_pct": kpis.get("thirtyDayChange", {}).get(
                        "value"
                    ),
                    "runDate": data.get("runDate"),
                }
        except (json.JSONDecodeError, KeyError) as e:
            logging.warning(f"Could not process KPI file {blob_name}: {e}")
            continue

    return latest_kpis


def _get_ticker_work_list() -> pd.DataFrame:
    """Gets the base metadata for the latest quarter for each ticker."""
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    query = f"""
        SELECT ticker, company_name, industry, sector, quarter_end_date
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY quarter_end_date DESC) as rn
            FROM `{config.BUNDLER_STOCK_METADATA_TABLE_ID}`
            WHERE ticker IS NOT NULL AND quarter_end_date IS NOT NULL
        ) WHERE rn = 1
    """
    return client.query(query).to_dataframe()


def _get_weighted_scores() -> pd.DataFrame:
    """Fetches the latest weighted_score and details for each ticker."""
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    query = f"""
        SELECT * EXCEPT(rn) FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY run_date DESC) as rn
            FROM `{config.BUNDLER_SCORES_TABLE_ID}`
            WHERE weighted_score IS NOT NULL
        ) WHERE rn = 1
    """
    return client.query(query).to_dataframe()


def _assemble_final_metadata(
    work_list_df: pd.DataFrame,
    scores_df: pd.DataFrame,
    daily_files_map: dict,
    kpis_map: dict,
) -> list[dict[str, Any]]:
    """Joins metadata and adds GCS asset URIs and KPIs."""
    if scores_df.empty:
        return []
    merged_df = pd.merge(work_list_df, scores_df, on="ticker", how="inner")
    final_records = []

    for _, row in merged_df.iterrows():
        ticker = row["ticker"]
        quarterly_date_str = row["quarter_end_date"].strftime("%Y-%m-%d")
        record = row.to_dict()

        record["news"] = daily_files_map.get(ticker, {}).get("news")
        record["recommendation_analysis"] = daily_files_map.get(ticker, {}).get(
            "recommendation_analysis"
        )
        record["pages_json"] = daily_files_map.get(ticker, {}).get("pages_json")
        record["dashboard_json"] = daily_files_map.get(ticker, {}).get("dashboard_json")
        record["price_chart_image_uri"] = daily_files_map.get(ticker, {}).get(
            "price_chart_image_uri"
        )  # Add new field

        record["technicals"] = (
            f"gs://{config.DESTINATION_GCS_BUCKET_NAME}/technicals/{ticker}_technicals.json"
        )
        record["profile"] = (
            f"gs://{config.DESTINATION_GCS_BUCKET_NAME}/sec-business/{ticker}_{quarterly_date_str}.json"
        )
        record["mda"] = (
            f"gs://{config.DESTINATION_GCS_BUCKET_NAME}/sec-mda/{ticker}_{quarterly_date_str}.json"
        )
        record["financials"] = (
            f"gs://{config.DESTINATION_GCS_BUCKET_NAME}/financial-statements/{ticker}_{quarterly_date_str}.json"
        )
        record["earnings_transcript"] = (
            f"gs://{config.DESTINATION_GCS_BUCKET_NAME}/earnings-call-transcripts/{ticker}_{quarterly_date_str}.json"
        )
        record["fundamentals"] = (
            f"gs://{config.DESTINATION_GCS_BUCKET_NAME}/fundamentals-analysis/{ticker}_{quarterly_date_str}.json"
        )

        record["image_uri"] = (
            f"gs://{config.DESTINATION_GCS_BUCKET_NAME}/images/{ticker}.png"
        )

        ticker_kpis = kpis_map.get(ticker, {})

        try:
            record["price"] = (
                float(ticker_kpis.get("price"))
                if ticker_kpis.get("price") is not None
                else None
            )
            record["thirty_day_change_pct"] = (
                float(ticker_kpis.get("thirty_day_change_pct"))
                if ticker_kpis.get("thirty_day_change_pct") is not None
                else None
            )
            record["weighted_score"] = (
                float(row.get("weighted_score"))
                if row.get("weighted_score") is not None
                else None
            )

            # --- New Fields for Agent ---
            record["score_percentile"] = (
                float(row.get("score_percentile"))
                if row.get("score_percentile") is not None
                else None
            )
            record["aggregated_text"] = row.get("aggregated_text")

            for score_col in [
                "news_score",
                "technicals_score",
                "fundamentals_score",
                "financials_score",
                "mda_score",
                "transcript_score",
            ]:
                if row.get(score_col) is not None:
                    try:
                        record[score_col] = float(row.get(score_col))
                    except (ValueError, TypeError):
                        record[score_col] = None

        except (ValueError, TypeError):
            record["price"] = record.get("price")
            record["thirty_day_change_pct"] = record.get("thirty_day_change_pct")
            record["weighted_score"] = record.get("weighted_score")

        weighted_score = record["weighted_score"]
        if weighted_score is not None:
            if weighted_score > 0.62:
                record["recommendation"] = "BUY"
            elif weighted_score >= 0.43:
                record["recommendation"] = "HOLD"
            else:
                record["recommendation"] = "SELL"
        else:
            record["recommendation"] = None

        final_records.append(record)
    return final_records


def _sync_bq_table(source_table_id: str, destination_table_id: str):
    """
    Efficiently copies a BigQuery table from source to destination using the Copy Job API.
    This avoids pulling data into memory and is suitable for large tables.
    """
    client = bigquery.Client(project=config.DESTINATION_PROJECT_ID)

    # Check if source exists (sanity check to avoid errors on empty source)
    source_client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    try:
        source_client.get_table(source_table_id)
    except Exception:
        logging.warning(
            f"Source table {source_table_id} does not exist. Skipping sync."
        )
        return

    job_config = bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")

    try:
        logging.info(f"Starting Copy Job: {source_table_id} -> {destination_table_id}")
        job = client.copy_table(
            source_table_id, destination_table_id, job_config=job_config
        )
        job.result()  # Wait for job to complete
        logging.info(f"Successfully copied table to {destination_table_id}")
    except Exception as e:
        logging.error(
            f"Failed to copy table {source_table_id} to {destination_table_id}: {e}",
            exc_info=True,
        )


def run_pipeline():
    """Orchestrates the final assembly and loading of asset metadata."""
    logging.info("--- Starting Data Bundler (Final Assembly) Pipeline ---")

    _sync_gcs_data()
    daily_files_map = _get_latest_daily_files_map()
    kpis_map = _get_latest_kpis()

    work_list_df = _get_ticker_work_list()
    if work_list_df.empty:
        logging.warning("No tickers in work list. Shutting down.")
        return

    scores_df = _get_weighted_scores()
    final_metadata = _assemble_final_metadata(
        work_list_df, scores_df, daily_files_map, kpis_map
    )

    if not final_metadata:
        logging.warning("No complete records to load to BigQuery.")
        return

    df = pd.DataFrame(final_metadata)

    # Use load_df_to_bq with WRITE_TRUNCATE to handle schema evolution (e.g. adding run_date)
    # and ensure the agent always sees a clean, up-to-date snapshot.
    bq.load_df_to_bq(
        df,
        config.BUNDLER_ASSET_METADATA_TABLE_ID,
        config.DESTINATION_PROJECT_ID,
        write_disposition="WRITE_TRUNCATE",
    )

    # --- Sync Calendar Events Table ---
    logging.info("Syncing Calendar Events table...")
    _sync_bq_table(
        config.SOURCE_CALENDAR_EVENTS_TABLE_ID,
        config.DESTINATION_CALENDAR_EVENTS_TABLE_ID,
    )

    # --- Sync Agent Tables (Winners, Options, Price) ---
    logging.info("Syncing Agent Data Tables...")
    _sync_bq_table(
        config.SOURCE_WINNERS_DASHBOARD_TABLE_ID,
        config.DESTINATION_WINNERS_DASHBOARD_TABLE_ID,
    )
    _sync_bq_table(
        config.SOURCE_OPTIONS_CHAIN_TABLE_ID, config.DESTINATION_OPTIONS_CHAIN_TABLE_ID
    )
    _sync_bq_table(
        config.SOURCE_PRICE_DATA_TABLE_ID, config.DESTINATION_PRICE_DATA_TABLE_ID
    )

    logging.info("--- Data Bundler (Final Assembly) Pipeline Finished ---")
