"""
Central configuration for all Enrichment services.
"""
import os

# --- Global Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- BigQuery ---
BQ_METADATA_TABLE = f"{PROJECT_ID}.profit_scout.stock_metadata"
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")

# --- Vertex AI Gen AI ---
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-2.0-flash")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.6"))
TOP_P = float(os.getenv("TOP_P", "0.95"))
TOP_K = int(os.getenv("TOP_K", "30"))
SEED = int(os.getenv("SEED", "42"))
CANDIDATE_COUNT = int(os.getenv("CANDIDATE_COUNT", "1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "512"))

# --- Cloud Storage Prefixes ---
PREFIXES = {
    "mda_summarizer": {"input": "sec-mda/", "output": "mda-summaries/"},
    "mda_analyzer": {"input": "mda-summaries/", "output": "mda-analysis/"},
    "transcript_summarizer": {"input": "earnings-call-transcripts/", "output": "earnings-call-summaries/"},
    "transcript_analyzer": {"input": "earnings-call-summaries/", "output": "transcript-analysis/"},
    "financials_analyzer": {"input": "financial-statements/", "output": "financials-analysis/"},
    "technicals_analyzer": {"input": "technicals/", "output": "technicals-analysis/"},
    "news_analyzer": {"input": "headline-news/", "output": "news-analysis/"},
    "news_fetcher": {"query_cache": "news-queries/"},
    "business_summarizer": {"input": "sec-business/", "output": "business-summaries/"},
    # --- THIS IS THE CORRECTED SECTION ---
    "fundamentals_analyzer": {
        "input_metrics": "key-metrics/", 
        "input_ratios": "ratios/", 
        "output": "fundamentals-analysis/"
    }
}

# --- Job Parameters ---
MAX_WORKERS = 8
HEADLINE_LIMIT = 25