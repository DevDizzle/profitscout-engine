# serving/core/config.py
import os

# --- Global Project/Service Configuration ---
SOURCE_PROJECT_ID = os.environ.get("PROJECT_ID", "profitscout-lx6bb")
DESTINATION_PROJECT_ID = os.environ.get("DESTINATION_PROJECT_ID", "profitscout-fida8")
LOCATION = os.environ.get("LOCATION", "us-central1")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "profit-scout-data")
DESTINATION_GCS_BUCKET_NAME = os.environ.get("DESTINATION_GCS_BUCKET_NAME", "profit-scout")
FMP_API_KEY_SECRET = os.environ.get("FMP_API_KEY_SECRET", "FMP_API_KEY")

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
    "metrics": "key-metrics-analysis/",
    "ratios": "ratios-analysis/",
}
SCORE_WEIGHTS = {
    "news_score": 0.22, "technicals_score": 0.28, "mda_score": 0.07,
    "transcript_score": 0.13, "financials_score": 0.12, "metrics_score": 0.09,
    "ratios_score": 0.09,
}

# --- Recommendation Generator Pipeline ---
RECOMMENDATION_PREFIX = "recommendations/"
MAX_WORKERS_RECOMMENDER = 8
PRICE_DATA_TABLE_ID = f"{SOURCE_PROJECT_ID}.profit_scout.price_data" # For chart data
CHART_GCS_FOLDER = "charts/" # GCS folder for chart images
SERVICE_ACCOUNT_EMAIL = os.environ.get(
    "SERVICE_ACCOUNT_EMAIL"
)

# --- Page Generator Pipeline ---
PAGE_JSON_PREFIX = "pages/"

# --- Data Bundler Pipeline ---
MAX_WORKERS_BUNDLER = 24
BQ_DATASET_BUNDLER = "profit_scout"
ASSET_METADATA_TABLE = "asset_metadata"
STOCK_METADATA_TABLE = "stock_metadata"
BUNDLER_ASSET_METADATA_TABLE_ID = f"{DESTINATION_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{ASSET_METADATA_TABLE}"
BUNDLER_STOCK_METADATA_TABLE_ID = f"{SOURCE_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{STOCK_METADATA_TABLE}"
BUNDLER_SCORES_TABLE_ID = f"{SOURCE_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{SCORES_TABLE_NAME}"

# --- Sync to Firestore Pipeline ---
FIRESTORE_COLLECTION = "tickers"
SYNC_FIRESTORE_TABLE_ID = f"{DESTINATION_PROJECT_ID}.{BQ_DATASET_BUNDLER}.{ASSET_METADATA_TABLE}"

# --- Image Fetcher Pipeline ---
IMAGE_GCS_FOLDER = "logos/"
TICKER_LIST_PATH = "tickerlist.txt"
IMAGE_FETCHER_BATCH_SIZE = 50

# --- Vertex AI (Shared) ---
MODEL_NAME = os.environ.get("MODEL_NAME", "gemini-2.0-flash")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.5"))
TOP_P = float(os.getenv("TOP_P", "0.95"))
TOP_K = int(os.getenv("TOP_K", "40"))
SEED = int(os.getenv("SEED", "42"))
CANDIDATE_COUNT = int(os.getenv("CANDIDATE_COUNT", "1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "2048"))