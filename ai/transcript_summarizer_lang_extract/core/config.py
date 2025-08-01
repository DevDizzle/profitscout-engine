import os

# --- GCP Project ---
PROJECT_ID        = os.getenv("PROJECT_ID", "profitscout-lx6bb")
LOCATION          = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")

# --- Vertex AI Gen AI ---
MODEL_NAME        = os.getenv("MODEL_NAME", "gemini-1.5-flash")  # Updated to match LangExtract compatibility
TEMPERATURE       = float(os.getenv("TEMPERATURE", "0.1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "8192"))
CANDIDATE_COUNT   = 1

# --- LangExtract Specific ---
LANGEXTRACT_MODEL_ID = os.getenv("LANGEXTRACT_MODEL_ID", "gemini-1.5-flash")  # Use gemini-1.5-pro for complex tasks if needed

# --- Cloud Storage ---
GCS_BUCKET          = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")
GCS_INPUT_PREFIX    = "earnings-call-transcripts/"
GCS_OUTPUT_PREFIX   = "earnings-call-summaries/"

# --- Job Parameters ---
MAX_WORKERS         = 12