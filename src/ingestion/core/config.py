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
# For populate_price_data
PRICE_DATA_TABLE = "price_data"
PRICE_DATA_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{PRICE_DATA_TABLE}"
# For SPY price sync
SPY_PRICE_TABLE = os.getenv("SPY_PRICE_TABLE", "spy_price_history")
SPY_PRICE_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{SPY_PRICE_TABLE}"
# For refresh_stock_metadata & transcript_collector
MASTER_TABLE = "stock_metadata"
MASTER_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{MASTER_TABLE}"
# For technicals_collector
TECHNICALS_PRICE_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{PRICE_DATA_TABLE}"
# For calendar events
CALENDAR_EVENTS_TABLE = "calendar_events"
CALENDAR_EVENTS_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{CALENDAR_EVENTS_TABLE}"

# --- Cloud Storage Prefixes (for News Fetcher) ---
PREFIXES = {
    # Path for the news analyzer to read from
    "news_analyzer": {"input": "headline-news/"},
    # Path for the news fetcher to cache its AI-generated queries
    "news_fetcher": {"query_cache": "news-queries/"},
}


# --- Fundamentals / Statements / Ratios ---
QUARTERS_TO_FETCH = 8
KEY_METRICS_FOLDER = "key-metrics/"
RATIOS_FOLDER = "ratios/"
FINANCIAL_STATEMENTS_FOLDER = "financial-statements/"

# --- Price Updater ---
PRICE_UPDATER_OUTPUT_FOLDER = "prices/"

# --- SEC Filing Extractor ---
BUSINESS_FOLDER = "sec-business/"
MDA_FOLDER = "sec-mda/"
RISK_FOLDER = "sec-risk/"
SECTION_MAP = {
    "10-K": {"business": "1", "mda": "7", "risk": "1A"},
    "10-KT": {"business": "1", "mda": "7", "risk": "1A"},
    "20-F": {"business": "item4"},
    "40-F": {"business": "1"},
    "10-Q": {"mda": "part1item2", "risk": "part2item1a"},
    "10-QT": {"mda": "part1item2", "risk": "part2item1a"}
}

# --- Transcript Collector ---
TRANSCRIPT_OUTPUT_FOLDER = "earnings-call-transcripts/"
PUB_SUB_TOPIC_ID = "new-transcript-created"

# --- Technicals Collector ---
TECHNICALS_OUTPUT_FOLDER = "technicals/"
ROLLING_52_WEEK_WINDOW = 252
INDICATORS = {
    "sma_50": {"kind": "sma", "params": {"length": 50}},
    "sma_200": {"kind": "sma", "params": {"length": 200}},
    "ema_21": {"kind": "ema", "params": {"length": 21}},
    "adx": {"kind": "adx", "params": {"length": 14}},
    "macd": {"kind": "macd", "params": {"fast": 12, "slow": 26, "signal": 9}},
    "rsi_14": {"kind": "rsi", "params": {"length": 14}},
    "stochastic": {"kind": "stoch", "params": {"k": 14, "d": 3, "smooth_k": 3}},
    "roc_20": {"kind": "roc", "params": {"length": 20}},
    "bollinger_bands": {"kind": "bbands", "params": {"length": 20, "std": 2}},
    "atr": {"kind": "atr", "params": {"length": 14}},
    "obv": {"kind": "obv", "params": {}}
}

# --- Populate Price Data ---
DEFAULT_START_DATE = datetime.date(2020, 1, 1)
SPY_DEFAULT_START_DATE = datetime.date(2025, 10, 21)
SPY_PRICE_FIRESTORE_COLLECTION = os.getenv("SPY_PRICE_FIRESTORE_COLLECTION", "spy_price_history")
DESTINATION_PROJECT_ID = os.getenv("DESTINATION_PROJECT_ID", "profitscout-fida8")

# --- Job Parameters (Workers / Batching) ---
MAX_WORKERS_TIERING = {
    "fundamentals": 6,
    "populate_price_data": 24,
    "price_updater": 10,
    "refresh_stock_metadata": 4,
    "sec_filing_extractor": 4,
    "statement_loader": 5,
    "technicals_collector": 8,
    "transcript_collector": 6
}
BATCH_SIZE = 100 # Used by populate_price_data and technicals_collector

# --- Vertex AI Gen AI ---
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-3-flash-preview")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.6"))
TOP_P = float(os.getenv("TOP_P", "0.95"))
TOP_K = int(os.getenv("TOP_K", "30"))
SEED = int(os.getenv("SEED", "42"))
CANDIDATE_COUNT = int(os.getenv("CANDIDATE_COUNT", "1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "512"))


# --- Options ---
OPTIONS_CHAIN_TABLE = "options_chain"
OPTIONS_CHAIN_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{OPTIONS_CHAIN_TABLE}"
