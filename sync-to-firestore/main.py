import json
import logging
import os
import functions_framework
from google.cloud import bigquery, firestore, storage

# --- Configuration from Environment Variables ---
# Use a specific variable for the project containing the BQ table and Firestore
DESTINATION_PROJECT_ID = os.environ.get("DESTINATION_PROJECT_ID")
BQ_METADATA_TABLE = os.environ.get("BQ_METADATA_TABLE")
FIRESTORE_COLLECTION = os.environ.get("FIRESTORE_COLLECTION", "stocks")

# --- Initialize Google Cloud Clients ---
# Specify the project for each client for clarity and correctness
db = firestore.Client(project=DESTINATION_PROJECT_ID)
bq_client = bigquery.Client(project=DESTINATION_PROJECT_ID)
storage_client = storage.Client(project=DESTINATION_PROJECT_ID)

# --- Data Completeness Configuration ---
# A bundle is only "complete" if it has all of these sections.
EXPECTED_SECTIONS = [
    "earnings_call_summary",
    "financial_statements",
    "key_metrics",
    "ratios",
    "technicals",
    "business_profile",
    "prices",
    "sec_mda"
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_bundle_from_gcs(gcs_path):
    """
    Downloads, parses, and validates a data bundle from a GCS path.

    Args:
        gcs_path (str): The full GCS path (e.g., gs://bucket/file.json).

    Returns:
        dict or None: The parsed JSON data if the bundle is valid and complete, otherwise None.
    """
    try:
        # Extract bucket and blob name from the GCS path
        bucket_name = gcs_path.split('/')[2]
        blob_name = '/'.join(gcs_path.split('/')[3:])
        
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            logging.warning(f"GCS object not found at path: {gcs_path}")
            return None

        # Download and parse the JSON content
        content = blob.download_as_text(encoding="utf-8")
        data = json.loads(content)
        
        # --- Validation Logic ---
        missing_sections = [section for section in EXPECTED_SECTIONS if section not in data]
        if missing_sections:
            logging.warning(f"SKIPPING Incomplete Bundle at {gcs_path}. Missing: {', '.join(missing_sections)}")
            return None
        
        return data

    except (json.JSONDecodeError, IndexError) as e:
        logging.error(f"Failed to parse or read bundle at {gcs_path}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred fetching bundle {gcs_path}: {e}")
        return None


@functions_framework.cloud_event
def run(cloud_event):
    """
    Triggered by a Pub/Sub topic.
    Queries BigQuery for asset metadata, validates each corresponding data bundle
    in GCS for completeness, and syncs only the complete bundles to Firestore.
    """
    logging.info("Sync-to-firestore job triggered. Fetching asset metadata from BigQuery.")
    
    query = f"""
        SELECT
            ticker, company_name, industry, sector, quarter_end_date,
            earnings_call_date, earnings_year, earnings_quarter,
            bundle_gcs_path, recommendation, recommendation_uri
        FROM
            `{DESTINATION_PROJECT_ID}.{BQ_METADATA_TABLE}`
        WHERE
            bundle_gcs_path IS NOT NULL
    """
    
    try:
        query_job = bq_client.query(query)
        rows = list(query_job.result()) # Consume iterator to get a count
        
        if not rows:
            logging.info("No asset metadata found in BigQuery to process.")
            return

        logging.info(f"Found {len(rows)} metadata records to process.")
        
        # --- Clear the entire Firestore collection first to ensure freshness ---
        logging.info("Clearing existing documents from Firestore collection to repopulate with fresh completes.")
        delete_batch = db.batch()
        docs = db.collection(FIRESTORE_COLLECTION).stream()
        delete_count = 0

        for doc in docs:
            delete_batch.delete(doc.reference)
            delete_count += 1
            
            # Commit delete batch every 500 documents
            if delete_count % 500 == 0:
                logging.info(f"Committing delete batch of {delete_count} documents...")
                delete_batch.commit()
                delete_batch = db.batch()

        # Commit any remaining deletes
        if delete_count % 500 != 0:
            logging.info(f"Committing final delete batch of {delete_count % 500} documents...")
            delete_batch.commit()

        logging.info(f"Deleted {delete_count} documents from Firestore. Now repopulating with completes.")
        
        batch = db.batch()
        synced_count = 0
        
        for row in rows:
            ticker = row.ticker
            gcs_path = row.bundle_gcs_path
            
            # --- Integration Point ---
            # Fetch and validate the bundle. If it's None, it was incomplete or failed.
            bundle_data = get_bundle_from_gcs(gcs_path)
            
            if bundle_data is None:
                continue # Skip to the next record

            # If the bundle is valid and complete, add it to the batch
            doc_ref = db.collection(FIRESTORE_COLLECTION).document(ticker)
            data_to_sync = {
                "company_name": row.company_name,
                "industry": row.industry,
                "sector": row.sector,
                "quarter_end_date": str(row.quarter_end_date) if row.quarter_end_date else None,
                "earnings_call_date": str(row.earnings_call_date) if row.earnings_call_date else None,
                "earnings_year": row.earnings_year,
                "earnings_quarter": row.earnings_quarter,
                "bundle_gcs_path": gcs_path,
                "recommendation": row.recommendation,
                "recommendation_uri": row.recommendation_uri,
                "last_synced": firestore.SERVER_TIMESTAMP,
                # Clear out old fields to ensure data consistency
                'quarters': firestore.DELETE_FIELD
            }
            batch.set(doc_ref, data_to_sync, merge=True)
            synced_count += 1
            
            # Commit the batch every 500 documents
            if synced_count > 0 and synced_count % 500 == 0:
                logging.info(f"Committing batch of {synced_count} documents...")
                batch.commit()
                batch = db.batch() # Start a new batch
        
        # Commit any remaining documents in the final batch
        if synced_count > 0 and synced_count % 500 != 0:
            logging.info(f"Committing final batch of {synced_count % 500} documents...")
            batch.commit()

        logging.info(f"Successfully synced {synced_count} COMPLETE documents to Firestore.")

    except Exception as e:
        logging.error(f"An error occurred during the main sync process: {e}")
        raise