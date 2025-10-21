# serving/core/pipelines/sync_winners_to_firestore.py
import logging
import pandas as pd
from google.cloud import firestore, bigquery
from .. import config
import numpy as np

# --- Configuration ---
BATCH_SIZE = 500
# --- THIS IS THE FIX: Changed primary key ---
PRIMARY_KEY_FIELD = "contract_symbol" # Changed from "ticker"
# The BigQuery table to sync from
SOURCE_TABLE_ID = "profitscout-lx6bb.profit_scout.winners_dashboard"
# The Firestore collection to sync to
FIRESTORE_COLLECTION_NAME = "winners_dashboard"


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


def _commit_ops(db, ops):
    """Commits a list of Firestore operations in batches."""
    batch = db.batch()
    count = 0
    for op in ops:
        if op["type"] == "set":
            batch.set(op["ref"], op["data"])
        elif op["type"] == "delete":
            batch.delete(op["ref"])
        count += 1
        if count >= BATCH_SIZE:
            batch.commit()
            batch = db.batch()
            count = 0
    if count:
        batch.commit()


def _delete_collection_in_batches(collection_ref):
    """Wipes all documents from a Firestore collection."""
    logging.info(f"Wiping Firestore collection: '{collection_ref.id}'...")
    deleted_count = 0
    while True:
        docs = list(collection_ref.limit(BATCH_SIZE).stream())
        if not docs:
            break
        ops = [{"type": "delete", "ref": d.reference} for d in docs]
        _commit_ops(firestore.Client(project=config.DESTINATION_PROJECT_ID), ops)
        deleted_count += len(ops)
        logging.info(f"Deleted {deleted_count} total docs...")
    logging.info(f"Wipe complete for collection '{collection_ref.id}'.")


def _load_bq_df(bq: bigquery.Client, query: str) -> pd.DataFrame:
    """Loads data from a BigQuery query into a pandas DataFrame and cleans it."""
    df = bq.query(query).to_dataframe()
    if not df.empty:
        # Convert date/time columns to string for Firestore compatibility
        for col in df.columns:
            dtype_str = str(df[col].dtype)
            if "datetime" in dtype_str or "dbdate" in dtype_str or "date" in dtype_str:
                df[col] = df[col].astype(str)

        # Replace pandas/numpy nulls with None for Firestore
        df = df.replace({pd.NA: np.nan}).where(pd.notna(df), None)
    return df


def run_pipeline(full_reset: bool = True):
    """
    Wipes and repopulates the 'winners_dashboard' collection in Firestore
    from the corresponding BigQuery table. Uses contract_symbol as the document ID.
    """
    db = firestore.Client(project=config.DESTINATION_PROJECT_ID)
    bq = bigquery.Client(project=config.SOURCE_PROJECT_ID) # Source BQ table is in SOURCE project

    collection_ref = db.collection(FIRESTORE_COLLECTION_NAME)
    logging.info(f"--- Winners Dashboard Sync Pipeline (Contract Level) ---")
    logging.info(f"Target collection: {collection_ref.id}")
    logging.info(f"Source table: {SOURCE_TABLE_ID}")
    logging.info(f"Firestore Document ID Key: {PRIMARY_KEY_FIELD}")


    if full_reset:
        _delete_collection_in_batches(collection_ref)

    try:
        # Simple query to select all data from the source table
        query = f"SELECT * FROM `{SOURCE_TABLE_ID}`"
        df = _load_bq_df(bq, query)
    except Exception as e:
        logging.critical(f"Failed to query BigQuery: {e}", exc_info=True)
        raise

    if df.empty:
        logging.warning("Source table is empty. Collection will remain empty.")
        return

    if PRIMARY_KEY_FIELD not in df.columns:
        raise ValueError(f"Primary key '{PRIMARY_KEY_FIELD}' not found in the table.")

    # Prepare documents for Firestore
    upsert_ops = []
    invalid_key_count = 0
    for _, row in df.iterrows():
        key = str(row[PRIMARY_KEY_FIELD])
        # Add basic validation for Firestore ID
        if not key or '/' in key:
            logging.warning(f"Skipping row with invalid Firestore ID (empty or contains '/'): {key}. Row data: {row.to_dict()}")
            invalid_key_count += 1
            continue
        doc_ref = collection_ref.document(key)
        upsert_ops.append({"type": "set", "ref": doc_ref, "data": row.to_dict()})

    if invalid_key_count > 0:
         logging.warning(f"Skipped {invalid_key_count} rows due to invalid Firestore document IDs.")

    if not upsert_ops:
        logging.warning("No valid documents to upsert after ID validation.")
        return

    logging.info(
        f"Populating '{collection_ref.id}' with {len(upsert_ops)} documents..."
    )
    for chunk in _iter_batches(upsert_ops, BATCH_SIZE):
        _commit_ops(db, chunk)

    logging.info(f"✅ Sync complete for '{collection_ref.id}'.")
    logging.info("--- Winners Dashboard Sync Pipeline Finished ---")