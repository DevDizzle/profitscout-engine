# serving/core/pipelines/sync_options_candidates_to_firestore.py
import logging
import pandas as pd
from google.cloud import firestore, bigquery
from .. import config
import numpy as np

# --- Configuration ---
BATCH_SIZE = 500
CANDIDATES_TABLE_ID = f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_candidates"
FIRESTORE_COLLECTION_NAME = "options_candidates"

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

def _delete_collection_in_batches(db: firestore.Client, collection_ref: firestore.CollectionReference):
    """Wipes all documents from a Firestore collection in batches."""
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
        logging.info(f"Deleted {deleted_count} docs...")
    logging.info(f"Wipe complete for collection '{collection_ref.id}'.")

def _load_bq_df(bq: bigquery.Client) -> pd.DataFrame:
    """
    Loads the latest batch of options candidates and prepares them for Firestore.
    """
    logging.info(f"Querying BigQuery table for the latest batch of candidates: {CANDIDATES_TABLE_ID}")
    
    # --- THIS IS THE NEW, MORE ROBUST QUERY ---
    # It finds the most recent timestamp in the table and fetches all records
    # matching that timestamp.
    query = f"""
        SELECT *
        FROM `{CANDIDATES_TABLE_ID}`
        WHERE selection_run_ts = (SELECT MAX(selection_run_ts) FROM `{CANDIDATES_TABLE_ID}`)
    """
    df = bq.query(query).to_dataframe()

    if not df.empty:
        # Convert date/time columns to string for Firestore compatibility.
        for col in df.columns:
            dtype_str = str(df[col].dtype)
            if "datetime" in dtype_str or "dbdate" in dtype_str or "timestamp" in dtype_str:
                df[col] = df[col].astype(str)

        df = df.replace({pd.NA: np.nan})
        df = df.where(pd.notna(df), None)
    return df

def run_pipeline():
    """
    Syncs the latest batch of options_candidates from BigQuery to a flat Firestore collection.
    This pipeline performs a full wipe-and-reload to ensure Firestore is an
    exact mirror of the latest candidates.
    """
    db = firestore.Client(project=config.DESTINATION_PROJECT_ID)
    bq = bigquery.Client(project=config.SOURCE_PROJECT_ID)

    collection_ref = db.collection(FIRESTORE_COLLECTION_NAME)
    logging.info("--- Options Candidates Firestore Sync Pipeline ---")
    logging.info(f"Source BQ Table: {CANDIDATES_TABLE_ID}")
    logging.info(f"Destination Firestore Collection: {collection_ref.id}")

    # For a daily snapshot of candidates, wiping each time is the cleanest approach.
    _delete_collection_in_batches(db, collection_ref)

    try:
        candidates_df = _load_bq_df(bq)
    except Exception as e:
        logging.critical(f"Failed to query candidates table from BigQuery: {e}", exc_info=True)
        raise

    if candidates_df.empty:
        logging.warning("No options candidates found in BigQuery. Firestore collection will be empty.")
        logging.info("--- Options Candidates Firestore Sync Pipeline Finished ---")
        return

    logging.info(f"Upserting {len(candidates_df)} documents to '{collection_ref.id}'...")

    total_written = 0
    for batch_rows in _iter_batches(candidates_df.iterrows(), BATCH_SIZE):
        batch = db.batch()
        for _, row in batch_rows:
            # Use the unique contract_symbol as the document ID
            doc_id = str(row["contract_symbol"])
            doc_ref = collection_ref.document(doc_id)
            batch.set(doc_ref, row.to_dict())

        batch.commit()
        total_written += len(batch_rows)
        logging.info(f"Wrote {total_written}/{len(candidates_df)} documents...")

    logging.info(f"Sync complete. Total documents written: {total_written}.")
    logging.info("--- Options Candidates Firestore Sync Pipeline Finished ---")