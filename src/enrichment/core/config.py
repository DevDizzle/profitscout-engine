"""
Central configuration for all Enrichment services.
"""
import os
import datetime

# --- Global Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- BigQuery ---
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")
# This is the line we're fixing. Renamed from BQ_METADATA_TABLE
STOCK_METADATA_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.stock_metadata"
SCORES_TABLE_NAME = "analysis_scores"
SCORES_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{SCORES_TABLE_NAME}"
OPTIONS_CHAIN_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.options_chain"
OPTIONS_CANDIDATES_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.options_candidates"
PRICE_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.price_data"

# --- Score Aggregator ---
SCORE_WEIGHTS = {
    "news_score": 0.375,
    "technicals_score": 0.475,
    "mda_score": 0.025,
    "transcript_score": 0.025,
    "financials_score": 0.05,
    "fundamentals_score": 0.05,
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
    "mda_analyzer": {"input": "sec-mda/", "output": "mda-analysis/"},
    "transcript_analyzer": {"input": "earnings-call-transcripts/", "output": "transcript-analysis/"},
    "financials_analyzer": {"input": "financial-statements/", "output": "financials-analysis/"},
    "technicals_analyzer": {"input": "technicals/", "output": "technicals-analysis/"},
    "news_analyzer": {"input": "headline-news/", "output": "news-analysis/"},
    "news_fetcher": {"query_cache": "news-queries/"},
    "business_summarizer": {"input": "sec-business/", "output": "business-summaries/"},
    "fundamentals_analyzer": {
        "input_metrics": "key-metrics/",
        "input_ratios": "ratios/",
        "output": "fundamentals-analysis/",
    },
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

# Timeout for worker processes in seconds
WORKER_TIMEOUT = 300

# --- Macro Thesis / Worldview ---
# Mode: "search" (Gemini + Google Search grounding) or "http" (your own HTTP sources)
MACRO_THESIS_MODE = "search"
MACRO_THESIS_MODEL_NAME = "gemini-2.5-pro"

# Default settings (required variables, even if unused in search mode)
MACRO_THESIS_MAX_SOURCES = 10
MACRO_THESIS_HTTP_TIMEOUT = 30
MACRO_THESIS_SOURCE_CHAR_LIMIT = 15000
MACRO_THESIS_SOURCES: list[dict] = []


def macro_thesis_blob_name() -> str:
    """
    Returns the destination path for the daily macro worldview.
    Saved at the bucket root (no folder).
    Format: macro_thesis_YYYY-MM-DD.json
    """
    today = datetime.date.today().isoformat()
    return f"macro_thesis_{today}.json"
