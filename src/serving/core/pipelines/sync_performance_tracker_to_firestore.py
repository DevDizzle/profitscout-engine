# serving/core/pipelines/sync_performance_tracker_to_firestore.py
import logging

import numpy as np
import pandas as pd
from google.cloud import bigquery, firestore

from .. import config

# --- Configuration ---
BATCH_SIZE = 500
TRACKER_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.performance_tracker"
)
FIRESTORE_COLLECTION_NAME = "performance_tracker"
SUMMARY_COLLECTION_NAME = "performance_summary"  # For the single average document


def _iter_batches(iterable, n):
    """Yield successive n-sized chunks from iterable."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch


def _delete_collection(
    db: firestore.Client, collection_ref: firestore.CollectionReference
):
    """Wipes all documents from a Firestore collection."""
    logging.info(f"Wiping Firestore collection: '{collection_ref.id}'...")
    deleted_count = 0
    while True:
        docs = list(collection_ref.limit(BATCH_SIZE).stream())
        if not docs:
            break
        batch = db.batch()
        for doc in docs:
            batch.delete(doc.reference)
        batch.commit()
        deleted_count += len(docs)
    logging.info(
        f"Wipe complete for collection '{collection_ref.id}'. Deleted {deleted_count} docs."
    )


def _load_bq_df(bq: bigquery.Client) -> pd.DataFrame:
    """Loads the performance tracker data and prepares it for Firestore."""
    logging.info(f"Querying BigQuery table: {TRACKER_TABLE_ID}")
    query = f"SELECT * FROM `{TRACKER_TABLE_ID}`"
    df = bq.query(query).to_dataframe()

    if not df.empty:
        # Convert date/time columns to string for Firestore compatibility.
        for col in df.columns:
            dtype_str = str(df[col].dtype)
            if (
                "datetime" in dtype_str
                or "dbdate" in dtype_str
                or "timestamp" in dtype_str
            ):
                df[col] = df[col].astype(str)
        # Handle NaN/NA values
        df = df.replace({pd.NA: np.nan}).where(pd.notna(df), None)
    return df


def run_pipeline():
    """
    Performs a full wipe-and-reload sync of the performance tracker data to Firestore.
    """
    db = firestore.Client(project=config.DESTINATION_PROJECT_ID)
    bq = bigquery.Client(project=config.SOURCE_PROJECT_ID)

    tracker_collection_ref = db.collection(FIRESTORE_COLLECTION_NAME)
    summary_collection_ref = db.collection(SUMMARY_COLLECTION_NAME)

    logging.info("--- Performance Tracker Firestore Sync Pipeline ---")

    # 1. Wipe both collections for a clean slate
    _delete_collection(db, tracker_collection_ref)
    _delete_collection(db, summary_collection_ref)

    try:
        tracker_df = _load_bq_df(bq)
    except Exception as e:
        logging.critical(
            f"Failed to query performance tracker table from BigQuery: {e}",
            exc_info=True,
        )
        raise

    if tracker_df.empty:
        logging.warning(
            "No performance data found in BigQuery. Firestore will be empty."
        )
        # Still write a default summary object
        summary_doc_ref = summary_collection_ref.document("summary")
        summary_doc_ref.set({"average_percent_gain": 0.0, "total_trades": 0})
        logging.info("--- Performance Tracker Firestore Sync Pipeline Finished ---")
        return

    # 2. Upload all individual trade records
    logging.info(
        f"Upserting {len(tracker_df)} documents to '{tracker_collection_ref.id}'..."
    )
    total_written = 0
    for batch_rows in _iter_batches(tracker_df.iterrows(), BATCH_SIZE):
        batch = db.batch()
        for _, row in batch_rows:
            doc_id = str(row["contract_symbol"])
            doc_ref = tracker_collection_ref.document(doc_id)
            batch.set(doc_ref, row.to_dict())
        batch.commit()
        total_written += len(batch_rows)
    logging.info(f"Wrote {total_written} individual performance records.")

    # 3. Calculate and upload the summary statistics
    logging.info("Calculating and uploading performance summary...")
    avg_gain = tracker_df["percent_gain"].mean()
    total_trades = len(tracker_df)

    summary_data = {
        "average_percent_gain": float(avg_gain) if pd.notna(avg_gain) else 0.0,
        "total_trades": int(total_trades),
    }

    summary_doc_ref = summary_collection_ref.document("summary")
    summary_doc_ref.set(summary_data)
    logging.info(f"Successfully uploaded summary data: {summary_data}")

    logging.info("--- Performance Tracker Firestore Sync Pipeline Finished ---")
