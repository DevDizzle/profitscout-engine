"""
Central configuration for the News Analyzer.
"""
import os

# --- GCP Project ---
PROJECT_ID        = os.getenv("PROJECT_ID", "profitscout-lx6bb")
LOCATION          = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")

# --- Vertex AI Gen AI ---
MODEL_NAME        = os.getenv("MODEL_NAME", "gemini-2.0-flash")
TEMPERATURE    = float(os.getenv("TEMPERATURE",    "0.6"))
TOP_P          = float(os.getenv("TOP_P",          "0.95"))
TOP_K          = int(  os.getenv("TOP_K",          "30"))
SEED           = int(  os.getenv("SEED",           "42"))
CANDIDATE_COUNT= int(  os.getenv("CANDIDATE_COUNT","1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS","2048"))

# --- Cloud Storage ---
GCS_BUCKET         = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")
GCS_INPUT_PREFIX   = "headline-news/"
GCS_OUTPUT_PREFIX  = "news-analysis/"

# --- Job Parameters ---
MAX_WORKERS = 8