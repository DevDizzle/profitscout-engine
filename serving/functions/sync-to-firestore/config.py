# serving/functions/sync-to-firestore/config.py
import os

# --- BigQuery Configuration ---
# This is the destination project ID where the final, assembled data lives.
BQ_PROJECT_ID = os.environ.get("DESTINATION_PROJECT_ID", "profitscout-fida8") 
BQ_DATASET = "profit_scout"
BQ_TABLE = "asset_metadata" 

# --- Firestore Configuration ---
# The ticker symbol will be used as the Document ID in the target collection.
FIRESTORE_COLLECTION = "tickers"