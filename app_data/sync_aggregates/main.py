import functions_framework
import os
import logging
import pandas as pd
from google.cloud import firestore, bigquery

# --- Configuration ---
DESTINATION_PROJECT_ID = os.environ.get("DESTINATION_PROJECT_ID")
BQ_INDUSTRY_TABLE = os.environ.get("BQ_INDUSTRY_TABLE")
BQ_SECTOR_TABLE = os.environ.get("BQ_SECTOR_TABLE")

# Initialize clients
db = firestore.Client(project=DESTINATION_PROJECT_ID)
bq_client = bigquery.Client(project=DESTINATION_PROJECT_ID)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def sync_table_to_firestore(table_id, collection_name, document_id_column):
    """
    Queries a BigQuery table and syncs its contents to a Firestore collection.
    """
    logging.info(f"Starting sync for table {table_id} to Firestore collection {collection_name}.")
    query = f"SELECT * FROM `{DESTINATION_PROJECT_ID}.{table_id}`"
    
    try:
        df = bq_client.query(query).to_dataframe()

        batch = db.batch()
        commit_count = 0
        for index, row in df.iterrows():
            document_id = row[document_id_column]
            doc_ref = db.collection(collection_name).document(document_id)
            
            # Convert row to dictionary for Firestore, handling timestamps
            data_to_sync = row.to_dict()
            if 'last_updated' in data_to_sync and pd.notna(data_to_sync['last_updated']):
                 data_to_sync['last_updated'] = data_to_sync['last_updated'].to_pydatetime()

            batch.set(doc_ref, data_to_sync, merge=True)
            commit_count += 1

            # Commit in batches of 500
            if commit_count % 500 == 0:
                batch.commit()
                batch = db.batch()

        # Commit any remaining docs
        if commit_count % 500 != 0:
            batch.commit()

        logging.info(f"Successfully synced {len(df)} documents to Firestore collection '{collection_name}'.")

    except Exception as e:
        logging.error(f"Failed to sync table {table_id}: {e}")
        raise

@functions_framework.cloud_event
def run(cloud_event):
    """
    Triggered by a Pub/Sub message from the data-aggregator.
    Syncs the aggregated BigQuery tables to Firestore.
    """
    # Sync the industries table
    sync_table_to_firestore(BQ_INDUSTRY_TABLE, "industries", "industry")
    
    # Sync the sectors table
    sync_table_to_firestore(BQ_SECTOR_TABLE, "sectors", "sector")

    return "OK", 200