import os

# --- GCP Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- BigQuery ---
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")
BIGQUERY_TABLE = os.getenv("MASTER_TABLE", "stock_metadata")
BIGQUERY_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

# --- Pub/Sub ---
PUB_SUB_TOPIC_ID = "new-transcript-created"

# --- API Keys ---
FMP_API_KEY_SECRET = os.getenv("FMP_API_KEY_SECRET", "FMP_API_KEY")

# --- Job Parameters ---
GCS_OUTPUT_FOLDER = "earnings-call-transcripts/"MAX_WORKERS = 12