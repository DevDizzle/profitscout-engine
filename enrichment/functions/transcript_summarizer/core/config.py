"""
Central configuration for the Transcript Summarizer.
"""
import os

# --- GCP Project ---
PROJECT_ID        = os.getenv("PROJECT_ID", "profitscout-lx6bb")
LOCATION          = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")

# --- Vertex AI Gen AI ---
MODEL_NAME        = os.getenv("MODEL_NAME", "gemini-2.0-flash")
TEMPERATURE       = float(os.getenv("TEMPERATURE", "0.1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "8192"))
CANDIDATE_COUNT   = 1

# core/config.py

# --- Cloud Storage ---
GCS_BUCKET         = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")
GCS_INPUT_PREFIX   = "earnings-call-transcripts/"  # <-- Use this for transcripts
GCS_OUTPUT_PREFIX  = "earnings-call-summaries/"    # <-- Use this for summaries

# --- Job Parameters ---
MAX_WORKERS = 8