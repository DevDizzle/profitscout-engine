# serving/core/pipelines/sync_to_firestore.py
import logging
import pandas as pd
from google.cloud import firestore, bigquery
from .. import config

def _delete_collection(coll_ref, batch_size: int):
    """Deletes all documents in a collection in batches."""
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0
    for doc in docs:
        doc.reference.delete()
        deleted += 1
    if deleted >= batch_size:
        return _delete_collection(coll_ref, batch_size)

def run_pipeline():
    """Orchestrates the sync from BigQuery to Firestore."""
    logging.info("--- Starting Firestore Sync Pipeline ---")
    db = firestore.Client()
    bq_client = bigquery.Client()
    collection_ref = db.collection(config.FIRESTORE_COLLECTION)

    logging.info(f"Wiping Firestore collection: '{config.FIRESTORE_COLLECTION}'...")
    _delete_collection(collection_ref, 200)
    logging.info("Wipe complete.")

    logging.info(f"Fetching scored rows from {config.SYNC_FIRESTORE_TABLE_ID}...")
    query = f"SELECT * FROM `{config.SYNC_FIRESTORE_TABLE_ID}` WHERE weighted_score IS NOT NULL"
    
    try:
        df = bq_client.query(query).to_dataframe()
        if df.empty:
            logging.info("No scored records found in BigQuery. Sync complete.")
            return
        
        for col in df.select_dtypes(include=['dbdate', 'datetime64[ns, UTC]']).columns:
            df[col] = df[col].astype(str)
    except Exception as e:
        logging.critical(f"Failed to query BigQuery: {e}", exc_info=True)
        raise

    batch = db.batch()
    total_docs_written = 0
    logging.info(f"Starting batch write of {len(df)} documents to Firestore...")
    for index, row in df.iterrows():
        doc_ref = collection_ref.document(str(row["ticker"]))
        doc_data = row.where(pd.notna(row), None).to_dict()
        batch.set(doc_ref, doc_data)
        
        if len(batch._mutations) >= 500:
            count = len(batch._mutations)
            batch.commit()
            total_docs_written += count
            logging.info(f"Committed a batch of {count} documents.")
            batch = db.batch()

    if len(batch._mutations) > 0:
        count = len(batch._mutations)
        batch.commit()
        total_docs_written += count
        logging.info(f"Committed final batch of {count} documents.")

    logging.info(f"✅ Successfully synced {total_docs_written} documents.")
    logging.info("--- Firestore Sync Pipeline Finished ---")