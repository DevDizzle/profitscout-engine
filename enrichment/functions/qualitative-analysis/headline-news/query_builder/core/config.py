"""
Central configuration for the Query Builder.
"""
import os

# --- GCP Project ---
PROJECT_ID        = os.getenv("PROJECT_ID", "profitscout-lx6bb")

# --- Cloud Storage ---
GCS_BUCKET          = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")
GCS_INPUT_PREFIX    = "profile-summaries/"
GCS_OUTPUT_PREFIX   = "profile-query/"

# --- Pub/Sub ---
INPUT_TOPIC_ID = "profile-summaries-created"
OUTPUT_TOPIC_ID = "queries-created"