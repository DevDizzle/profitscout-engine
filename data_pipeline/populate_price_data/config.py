import os
import datetime

# --- GCP Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- BigQuery ---
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "price_data")
BIGQUERY_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

# --- API Keys ---
FMP_API_KEY_SECRET = os.getenv("FMP_API_KEY_SECRET", "FMP_API_KEY")

# --- Job Parameters ---
TICKER_FILE_PATH = "tickerlist.txt"
DEFAULT_START_DATE = datetime.date(2020, 1, 1)
MAX_WORKERS = 24BATCH_SIZE = 100