"""
Central configuration for the News Fetcher.
"""
import os

# --- GCP Project ---
PROJECT_ID        = os.getenv("PROJECT_ID", "profitscout-lx6bb")

# --- FMP API ---
# The name of the secret in Secret Manager containing your FMP API Key
FMP_API_KEY_SECRET = os.getenv("FMP_API_KEY_SECRET", "FMP_API_KEY")

# --- Cloud Storage ---
GCS_BUCKET          = os.getenv("DATA_BUCKET", "profit-scout-data")
GCS_OUTPUT_PREFIX   = "headline-news/"

# --- Job Parameters ---
DOMAINS = 120  # Limit for each news endpoint