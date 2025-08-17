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
    
    db = firestore.Client(project=config.DESTINATION_PROJECT_ID)
    bq_client = bigquery.Client(project=config.DESTINATION_PROJECT_ID)
    
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
    commit_counter = 0  # Initialize a counter for the batch size
    total_docs_written = 0
    
    logging.info(f"Starting batch write of {len(df)} documents to Firestore...")
    for index, row in df.iterrows():
        doc_ref = collection_ref.document(str(row["ticker"]))
        doc_data = row.where(pd.notna(row), None).to_dict()
        batch.set(doc_ref, doc_data)
        commit_counter += 1 # Increment the counter
        
        # Check the counter instead of the private attribute
        if commit_counter >= 500:
            batch.commit()
            total_docs_written += commit_counter
            logging.info(f"Committed a batch of {commit_counter} documents.")
            batch = db.batch()  # Start a new batch
            commit_counter = 0  # Reset the counter

    # Commit any remaining documents in the final batch
    if commit_counter > 0:
        batch.commit()
        total_docs_written += commit_counter
        logging.info(f"Committed final batch of {commit_counter} documents.")

    logging.info(f"✅ Successfully synced {total_docs_written} documents.")
    logging.info("--- Firestore Sync Pipeline Finished ---")