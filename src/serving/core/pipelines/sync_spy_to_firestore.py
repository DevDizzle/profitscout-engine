# serving/core/pipelines/sync_spy_to_firestore.py
import logging
import pandas as pd
from google.cloud import firestore, bigquery
from .. import config
import datetime

BATCH_SIZE = 500

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
    """Commit a batch of write operations."""
    batch = db.batch()
    for op in ops:
        batch.set(op["ref"], op["data"])
    batch.commit()

def _delete_collection(db, collection_ref):
    """Wipes the collection to ensure a clean sync."""
    logging.info(f"Wiping collection: {collection_ref.id}")
    while True:
        docs = list(collection_ref.limit(BATCH_SIZE).stream())
        if not docs:
            break
        batch = db.batch()
        for doc in docs:
            batch.delete(doc.reference)
        batch.commit()

def run_pipeline():
    """Syncs SPY price history from BigQuery to Firestore."""
    logging.info("--- Starting SPY Price Firestore Sync ---")
    
    # Initialize clients for the serving layer
    db = firestore.Client(project=config.DESTINATION_PROJECT_ID)
    bq = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    
    collection_ref = db.collection(config.SPY_PRICE_FIRESTORE_COLLECTION)

    # 1. Load from BigQuery
    query = f"SELECT * FROM `{config.SPY_PRICE_TABLE_ID}` ORDER BY date ASC"
    try:
        df = bq.query(query).to_dataframe()
    except Exception as e:
        logging.critical(f"Failed to query SPY prices from {config.SPY_PRICE_TABLE_ID}: {e}", exc_info=True)
        raise

    if df.empty:
        logging.warning("No SPY prices found in BigQuery.")
        return

    # 2. Convert date objects to strings for Firestore
    if "date" in df.columns:
        df["date"] = df["date"].apply(
            lambda d: d.isoformat() if isinstance(d, datetime.date) else str(d)
        )
    
    # 3. Wipe the collection for a clean slate
    _delete_collection(db, collection_ref)
    
    # 4. Prepare Batch Writes
    ops = []
    for _, row in df.iterrows():
        doc_data = row.to_dict()
        # Use the date as the document ID (YYYY-MM-DD)
        doc_id = str(doc_data.get("date"))
        doc_ref = collection_ref.document(doc_id)
        ops.append({"ref": doc_ref, "data": doc_data})

    logging.info(f"Upserting {len(ops)} documents to '{config.SPY_PRICE_FIRESTORE_COLLECTION}'...")
    
    count = 0
    for batch in _iter_batches(ops, BATCH_SIZE):
        _commit_ops(db, batch)
        count += len(batch)

    logging.info(f"--- SPY Price Firestore Sync Finished. Written {count} docs. ---")