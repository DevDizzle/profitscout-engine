# fundamentals/config.py
import os

# --- GCP Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- API Keys ---
# The name of the secret in Google Secret Manager
FMP_API_KEY_SECRET = os.getenv("FMP_API_KEY_SECRET", "FMP_API_KEY")

# --- Job Parameters ---
TICKER_LIST_PATH = "tickerlist.txt" 
KEY_METRICS_FOLDER = "key-metrics/"
RATIOS_FOLDER = "ratios/"
QUARTERS_TO_FETCH = 8
MAX_WORKERS = 6