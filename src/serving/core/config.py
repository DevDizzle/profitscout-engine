# serving/core/config.py
import os

# --- Global Project/Service Configuration ---
SOURCE_PROJECT_ID = os.environ.get("PROJECT_ID", "profitscout-lx6bb")
DESTINATION_PROJECT_ID = os.environ.get("DESTINATION_PROJECT_ID", "profitscout-fida8")
LOCATION = os.environ.get("LOCATION", "us-central1")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "profit-scout-data")
DESTINATION_GCS_BUCKET_NAME = os.environ.get("DESTINATION_GCS_BUCKET_NAME", "profit-scout")
FMP_API_KEY_SECRET = os.environ.get("FMP_API_KEY_SECRET", "FMP_API_KEY")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")

# --- Score Aggregator Pipeline ---
BQ_DATASET_AGGREGATOR = "profit_scout"
SCORES_TABLE_NAME = "analysis_scores"
SCORES_TABLE_ID = f"{SOURCE_PROJECT_ID}.{BQ_DATASET_AGGREGATOR}.{SCORES_TABLE_NAME}"
ANALYSIS_PREFIXES = {
    "business_summary": "business-summaries/",
    "news": "news-analysis/",
    "technicals": "technicals-analysis/",
    "mda": "mda-analysis/",
    "transcript": "transcript-analysis/",
    "financials": "financials-analysis/",
    "fundamentals": "fundamentals-analysis/",
}
SCORE_WEIGHTS = {
    "news_score": 0.25,
    "technicals_score": 0.40,
    "mda_score": 0.05,
    "transcript_score": 0.07,
    "financials_score": 0.08,
    "fundamentals_score": 0.15,
}

# --- Recommendation Generator Pipeline ---
RECOMMENDATION_PREFIX = "recommendations/"
MAX_WORKERS_RECOMMENDER = 8
PRICE_DATA_TABLE_ID = f"{SOURCE_PROJECT_ID}.profit_scout.price_data"
SERVICE_ACCOUNT_EMAIL = os.environ.get("SERVICE_ACCOUNT_EMAIL")

# --- Chart Generator ---
CHART_GCS_FOLDER = "charts/"

# --- Options Explainer (Serving) ---
OPTIONS_CANDIDATES_TABLE_ID = f"{SOURCE_PROJECT_ID}.{BIGQUERY_DATASET}.options_candidates"
OPTIONS_MD_PREFIX = os.environ.get("OPTIONS_MD_PREFIX", "options-recommendations/")

# --- Page Generator Pipeline ---
PAGE_JSON_PREFIX = "pages/"

# --- Data Bundler Pipeline ---
MAX_WORKERS_BUNDLER = 48
BQ_DATASET_BUNDLER = "profit_scout"
ASSET_METADATA_TABLE = "asset_metadata"
STOCK_METADATA_TABLE = "stock_metadata"
CALENDAR_EVENTS_TABLE = "calendar_events"

BUNDLER_ASSET_METADATA_TABLE_ID = f"{DESTINATION_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{ASSET_METADATA_TABLE}"
BUNDLER_STOCK_METADATA_TABLE_ID = f"{SOURCE_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{STOCK_METADATA_TABLE}"
BUNDLER_SCORES_TABLE_ID = f"{SOURCE_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{SCORES_TABLE_NAME}"

SOURCE_CALENDAR_EVENTS_TABLE_ID = f"{SOURCE_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{CALENDAR_EVENTS_TABLE}"
DESTINATION_CALENDAR_EVENTS_TABLE_ID = f"{DESTINATION_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{CALENDAR_EVENTS_TABLE}"

# Additional tables for Agent
WINNERS_DASHBOARD_TABLE = "winners_dashboard"
OPTIONS_CHAIN_TABLE = "options_chain"
PRICE_DATA_TABLE = "price_data"

SOURCE_WINNERS_DASHBOARD_TABLE_ID = f"{SOURCE_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{WINNERS_DASHBOARD_TABLE}"
DESTINATION_WINNERS_DASHBOARD_TABLE_ID = f"{DESTINATION_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{WINNERS_DASHBOARD_TABLE}"

SOURCE_OPTIONS_CHAIN_TABLE_ID = f"{SOURCE_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{OPTIONS_CHAIN_TABLE}"
DESTINATION_OPTIONS_CHAIN_TABLE_ID = f"{DESTINATION_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{OPTIONS_CHAIN_TABLE}"

SOURCE_PRICE_DATA_TABLE_ID = f"{SOURCE_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{PRICE_DATA_TABLE}"
DESTINATION_PRICE_DATA_TABLE_ID = f"{DESTINATION_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{PRICE_DATA_TABLE}"

# --- Sync to Firestore Pipeline ---
FIRESTORE_COLLECTION = "tickers"
SYNC_FIRESTORE_TABLE_ID = f"{DESTINATION_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{ASSET_METADATA_TABLE}"

# --- Image Fetcher Pipeline ---
IMAGE_GCS_FOLDER = "logos/"
TICKER_LIST_PATH = "tickerlist.txt"
IMAGE_FETCHER_BATCH_SIZE = 50

# --- Vertex AI (Shared) ---
MODEL_NAME = os.environ.get("MODEL_NAME", "gemini-3-flash-preview")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.7"))
TOP_P = float(os.getenv("TOP_P", "0.95"))
TOP_K = int(os.getenv("TOP_K", "40"))
SEED = int(os.getenv("SEED", "42"))
CANDIDATE_COUNT = int(os.getenv("CANDIDATE_COUNT", "1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "2048"))

# --- Performance Tracker Pipeline ---
OUTPUT_TABLE_ID = f"{SOURCE_PROJECT_ID}.{BIGQUERY_DATASET}.performance_tracker"

SPY_PRICE_TABLE_ID = f"{SOURCE_PROJECT_ID}.{BIGQUERY_DATASET}.spy_price_history"
SPY_PRICE_FIRESTORE_COLLECTION = "spy_price_history"

# --- ML Predictions (NEW) ---
DAILY_PREDICTIONS_TABLE_ID = f"{SOURCE_PROJECT_ID}.{BIGQUERY_DATASET}.daily_predictions"

# --- Social Media (X/Twitter) ---
X_API_KEY = os.environ.get("X_API_KEY")
X_API_SECRET = os.environ.get("X_API_SECRET")
X_ACCESS_TOKEN = os.environ.get("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = os.environ.get("X_ACCESS_TOKEN_SECRET")
SOCIAL_MEDIA_HISTORY_COLLECTION = "social_media_history"