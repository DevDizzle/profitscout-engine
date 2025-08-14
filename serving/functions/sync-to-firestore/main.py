# serving/functions/sync-to-firestore/main.py

import functions_framework
import pandas as pd
from google.cloud import firestore, bigquery
from . import config

# Initialize clients globally to be reused across function invocations
db = firestore.Client()
bq_client = bigquery.Client()

def delete_collection(coll_ref, batch_size: int):
    """Deletes all documents in a specified collection in batches."""
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0
    for doc in docs:
        doc.reference.delete()
        deleted += 1
    # Recurse until the collection is empty
    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size)

@functions_framework.cloud_event
def sync_to_firestore(cloud_event):
    """
    Triggered by a Pub/Sub message, this function wipes the target Firestore 
    collection and syncs it with complete, scored records from the final 
    BigQuery asset_metadata table.
    """
    full_table_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET}.{config.BQ_TABLE}"
    collection_id = config.FIRESTORE_COLLECTION
    
    print(f"Firestore Sync triggered. Source BQ Table: {full_table_id}")

    # Step 1: Delete all existing documents in the target Firestore collection
    # This ensures the Firestore data is always a fresh mirror of the BQ table.
    collection_ref = db.collection(collection_id)
    print(f"Wiping all documents in Firestore collection: '{collection_id}'...")
    delete_collection(collection_ref, 200) # Deleting in batches of 200
    print("Firestore collection wipe complete.")

    # Step 2: Query BigQuery for records that have been scored
    # The presence of a non-null weighted_score acts as our "ready-to-sync" flag.
    print(f"Fetching scored rows from {full_table_id}...")
    query = f"""
        SELECT *
        FROM `{full_table_id}`
        WHERE weighted_score IS NOT NULL
    """
    
    try:
        df = bq_client.query(query).to_dataframe()
        if df.empty:
            print("No new scored records found in BigQuery. Firestore will not be populated.")
            return "Sync complete: No data to sync.", 200
        
        # Convert any date/datetime columns to strings to prevent Firestore errors.
        for col in df.select_dtypes(include=['dbdate', 'datetime64[ns, UTC]']).columns:
            df[col] = df[col].astype(str)

    except Exception as e:
        print(f"❌ An error occurred querying BigQuery: {e}")
        raise

    # Step 3: Write the new data from the DataFrame to Firestore in batches
    batch = db.batch()
    docs_in_batch = 0
    total_docs_written = 0
    
    print(f"Starting batch write of {len(df)} documents to Firestore...")
    for index, row in df.iterrows():
        # Use the ticker as the unique Document ID for easy lookups in the app
        doc_ref = collection_ref.document(str(row["ticker"]))
        
        # Convert the row to a dictionary, replacing any Pandas NaNs with None
        doc_data = row.where(pd.notna(row), None).to_dict()

        batch.set(doc_ref, doc_data)
        docs_in_batch += 1
        
        # Commit the batch every 500 documents to stay within Firestore limits
        if docs_in_batch >= 500:
            batch.commit()
            total_docs_written += docs_in_batch
            print(f"Committed a batch of {docs_in_batch} documents.")
            batch = db.batch() # Start a new batch
            docs_in_batch = 0

    # Commit any remaining documents in the final batch
    if docs_in_batch > 0:
        batch.commit()
        total_docs_written += docs_in_batch
        print(f"Committed the final batch of {docs_in_batch} documents.")

    print(f"✅ Successfully synced {total_docs_written} documents to Firestore.")
    return f"Sync successful.", 200