# transcript_summarizer/core/config.py
"""
Central configuration for the Transcript Summarizer.

Every value can be overridden with an environment variable
of the same name (handy for Cloud Functions gen 2).
"""
import os

# ── Vertex AI Gen AI ───────────────────────────────────────────
PROJECT_ID        = os.getenv("PROJECT_ID", "profitscout-lx6bb")
LOCATION          = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
MODEL_NAME        = os.getenv("MODEL_NAME", "gemini-2.0-flash-001")

TEMPERATURE       = float(os.getenv("TEMPERATURE", "0.1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "8192"))
CANDIDATE_COUNT   = 1

# ── Cloud Storage ──────────────────────────────────────────────
GCS_BUCKET        = os.getenv("GCS_BUCKET", "profit-scout-data")
GCS_PREFIX        = os.getenv("GCS_PREFIX", "earnings-call-summaries") 
TICKER_LIST_PATH  = os.getenv("TICKER_LIST_PATH", "tickerlist.txt")