import os

# --- GCP Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- API Keys ---
FMP_API_KEY_SECRET = os.getenv("FMP_API_KEY_SECRET", "FMP_API_KEY")

# --- Job Parameters ---
TICKER_LIST_PATH = "tickerlist.txt"
GCS_OUTPUT_FOLDER = "prices/"MAX_WORKERS = 10