"""
Central configuration for all Enrichment services.
"""
import os

# --- Global Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- BigQuery ---
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")
BQ_METADATA_TABLE = f"{PROJECT_ID}.{BIGQUERY_DATASET}.stock_metadata"
SCORES_TABLE_NAME = "analysis_scores"
SCORES_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{SCORES_TABLE_NAME}"


# --- Score Aggregator ---
SCORE_WEIGHTS = {
    "news_score": 0.25,
    "technicals_score": 0.40,
    "mda_score": 0.05,
    "transcript_score": 0.07,
    "financials_score": 0.08,
    "fundamentals_score": 0.15,
}

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
    "fundamentals_analyzer": {
        "input_metrics": "key-metrics/", 
        "input_ratios": "ratios/", 
        "output": "fundamentals-analysis/"
    }
}

ANALYSIS_PREFIXES = {
    "business_summary": "business-summaries/", 
    "news": "news-analysis/",
    "technicals": "technicals-analysis/",
    "mda": "mda-analysis/",
    "transcript": "transcript-analysis/",
    "financials": "financials-analysis/",
    "fundamentals": "fundamentals-analysis/",
}

# --- Job Parameters ---
MAX_WORKERS = 8
HEADLINE_LIMIT = 25