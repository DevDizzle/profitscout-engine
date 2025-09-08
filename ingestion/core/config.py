# ingestion/core/config.py
import os
import datetime

# --- Global Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")
TICKER_LIST_PATH = "tickerlist.txt"

# --- API Key Secret Names ---
FMP_API_KEY_SECRET = os.getenv("FMP_API_KEY_SECRET", "FMP_API_KEY")
SEC_API_KEY_SECRET = os.getenv("SEC_API_KEY_SECRET", "SEC_API_KEY")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")  

# --- BigQuery ---
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")
PRICE_DATA_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.price_data"
MASTER_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.stock_metadata"
TECHNICALS_PRICE_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.price_data"

# --- Cloud Storage Prefixes ---
# SIMPLIFIED: Only the news_analyzer output path is needed now.
PREFIXES = {
    "news_analyzer": {"input": "headline-news/"},
}

# --- Fundamentals / Statements / Ratios ---
# ... (rest of the file is the same) ...

# --- Job Parameters (Workers / Batching) ---
MAX_WORKERS_TIERING = {
    "fundamentals": 6,
    "populate_price_data": 24,
    "price_updater": 10,
    "refresh_stock_metadata": 4,
    "sec_filing_extractor": 4,
    "statement_loader": 5,
    "technicals_collector": 8,
    "transcript_collector": 6,
    "news_fetcher": 8
}
BATCH_SIZE = 100

# --- Vertex AI Gen AI ---
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-2.0-flash")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.6"))
TOP_P = float(os.getenv("TOP_P", "0.95"))
TOP_K = int(os.getenv("TOP_K", "30"))
SEED = int(os.getenv("SEED", "42"))
CANDIDATE_COUNT = int(os.getenv("CANDIDATE_COUNT", "1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "512"))