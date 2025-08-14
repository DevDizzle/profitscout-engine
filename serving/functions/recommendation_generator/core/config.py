# serving/functions/recommendation_generator/core/config.py
import os

# --- GCP Configuration ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
LOCATION = os.getenv("LOCATION", "us-central1")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- BigQuery Configuration ---
BQ_DATASET = "profit_scout"
SCORES_TABLE_NAME = "analysis_scores"
SCORES_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET}.{SCORES_TABLE_NAME}"

# --- GCS Configuration ---
RECOMMENDATION_PREFIX = "recommendations/"

# --- Job Parameters ---
MAX_WORKERS = 8

# --- Vertex AI Gen AI ---
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-2.0-flash")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.5"))
TOP_P = float(os.getenv("TOP_P", "0.95"))
TOP_K = int(os.getenv("TOP_K", "40"))
SEED = int(os.getenv("SEED", "42"))
CANDIDATE_COUNT = int(os.getenv("CANDIDATE_COUNT", "1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "1024"))