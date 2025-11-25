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
STOCK_METADATA_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.stock_metadata"
SCORES_TABLE_NAME = "analysis_scores"
SCORES_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{SCORES_TABLE_NAME}"

# Options Tables
OPTIONS_CHAIN_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.options_chain"
OPTIONS_CANDIDATES_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.options_candidates"
PRICE_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.price_data"

# --- NEW: Aliases for pipelines that use short names ---
CHAIN_TABLE = OPTIONS_CHAIN_TABLE_ID
CAND_TABLE = OPTIONS_CANDIDATES_TABLE_ID

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

# --- Pipeline Specific Models ---
TECHNICALS_ANALYZER_MODEL_NAME = os.getenv("TECHNICALS_ANALYZER_MODEL_NAME", "gemini-2.5-pro")
NEWS_ANALYZER_MODEL_NAME = os.getenv("NEWS_ANALYZER_MODEL_NAME", "gemini-2.5-pro")

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
WORKER_TIMEOUT = 300

# --- Macro Thesis / Worldview ---
MACRO_THESIS_MODE = "search"
MACRO_THESIS_MODEL_NAME = "gemini-2.5-pro"
MACRO_THESIS_MAX_SOURCES = 10
MACRO_THESIS_HTTP_TIMEOUT = 30
MACRO_THESIS_SOURCE_CHAR_LIMIT = 15000
MACRO_THESIS_SOURCES: list[dict] = []

def macro_thesis_blob_name() -> str:
    return "macro_thesis.txt"