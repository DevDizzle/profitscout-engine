# transcript_summarizer/core/config.py
"""
Central configuration (override with env vars).
"""
import os

# Vertex AI
PROJECT_ID        = os.getenv("PROJECT_ID", "profitscout-lx6bb")
LOCATION          = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
MODEL_NAME        = os.getenv("MODEL_NAME", "gemini-2.0-flash-001")
TEMPERATURE       = float(os.getenv("TEMPERATURE", "0.1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "8192"))
CANDIDATE_COUNT   = 1

# Cloud Storage paths
GCS_BUCKET        = os.getenv("GCS_BUCKET", "profit-scout-data")
GCS_INPUT_PREFIX  = os.getenv("GCS_INPUT_PREFIX",  "earnings-call-transcripts/")
GCS_OUTPUT_PREFIX = os.getenv("GCS_OUTPUT_PREFIX", "earnings-call-summaries/")

# Optional ticker list (set empty string to disable filtering)
TICKER_LIST_PATH  = os.getenv("TICKER_LIST_PATH", "tickerlist.txt")

# Parallelism inside the Cloud Function
MAX_THREADS       = int(os.getenv("MAX_THREADS", "6"))
