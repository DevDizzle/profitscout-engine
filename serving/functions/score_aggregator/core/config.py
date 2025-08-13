import os
from datetime import datetime

# --- GCP Configuration ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
LOCATION = os.getenv("LOCATION", "us-central1") # Kept for consistency, though vertex_ai.py may override it.
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- BigQuery Configuration ---
BQ_DATASET = "profit_scout"
SCORES_TABLE_NAME = "analysis_scores"
SCORES_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET}.{SCORES_TABLE_NAME}"

# --- GCS Configuration ---
ANALYSIS_PREFIXES = {
    "news": "news-analysis/",
    "technicals": "technicals-analysis/",
    "mda": "mda-analysis/",
    "transcript": "transcript-analysis/",
    "financials": "financials-analysis/",
    "metrics": "key-metrics-analysis/",
    "ratios": "ratios-analysis/",
}
RECOMMENDATION_PREFIX = "recommendations/"

# --- Synthesizer Configuration ---
# Your updated weights have been included.
SCORE_WEIGHTS = {
    "news_score": 0.22,
    "technicals_score": 0.28,
    "mda_score": 0.07,
    "transcript_score": 0.13,
    "financials_score": 0.12,
    "metrics_score": 0.09,
    "ratios_score": 0.09,
}

# --- Vertex AI Gen AI ---
# Renamed MODEL_NAME to LLM_MODEL_NAME to match the existing pattern.
LLM_MODEL_NAME = os.getenv("MODEL_NAME", "gemini-2.0-flash")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.6"))
TOP_P = float(os.getenv("TOP_P", "0.95"))
TOP_K = int(os.getenv("TOP_K", "30"))
SEED = int(os.getenv("SEED", "42"))
CANDIDATE_COUNT = int(os.getenv("CANDIDATE_COUNT", "1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "2048"))