# sync-to-firestore/main.py

import logging
import os
import functions_framework
from google.cloud import bigquery, firestore

# --- Configuration from Environment Variables ---
# Use a specific variable for the project containing the BQ table and Firestore
DESTINATION_PROJECT_ID = os.environ.get("DESTINATION_PROJECT_ID")
BQ_METADATA_TABLE = os.environ.get("BQ_METADATA_TABLE")
FIRESTORE_COLLECTION = os.environ.get("FIRESTORE_COLLECTION", "stocks")

# Initialize clients, specifying the project for Firestore
db = firestore.Client(project=DESTINATION_PROJECT_ID)
bq_client = bigquery.Client(project=DESTINATION_PROJECT_ID)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@functions_framework.cloud_event
def run(cloud_event):
    """
    Triggered by the data-bundler-complete topic.
    Queries the asset_metadata table to get all bundle paths and syncs them to Firestore.
    """
    logging.info("Sync-to-firestore job triggered. Fetching all asset metadata from BigQuery.")
    
    # Query the BigQuery table in the correct destination project
    query = f"""
        SELECT
            ticker,
            bundle_gcs_path
        FROM
            `{DESTINATION_PROJECT_ID}.{BQ_METADATA_TABLE}`
        WHERE
            bundle_gcs_path IS NOT NULL
    """
    
    try:
        query_job = bq_client.query(query)
        rows = query_job.result()

        batch = db.batch()
        count = 0
        for row in rows:
            ticker = row.ticker
            doc_ref = db.collection(FIRESTORE_COLLECTION).document(ticker)
            batch.set(doc_ref, {
                "bundle_gcs_path": row.bundle_gcs_path,
                "last_synced": firestore.SERVER_TIMESTAMP
            }, merge=True)
            count += 1
            
            # Commit the batch every 500 documents
            if count % 500 == 0:
                logging.info(f"Committing batch of {count} documents...")
                batch.commit()
                batch = db.batch()
        
        # Commit any remaining documents
        if count > 0 and count % 500 != 0:
            logging.info(f"Committing final batch of {count % 500} documents...")
            batch.commit()

        logging.info(f"Successfully synced {count} documents to Firestore collection '{FIRESTORE_COLLECTION}'.")

    except Exception as e:
        logging.error(f"An error occurred during Firestore sync: {e}")
        raise