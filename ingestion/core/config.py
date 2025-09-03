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
# For refresh_stock_metadata & transcript_collector
MASTER_TABLE = "stock_metadata"
MASTER_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{MASTER_TABLE}"
# For technicals_collector
TECHNICALS_PRICE_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{PRICE_DATA_TABLE}"



# --- Fundamentals / Statements / Ratios ---
QUARTERS_TO_FETCH = 4
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
    "obv": {"kind": "obv", "params": {}},
}

# --- Populate Price Data ---
DEFAULT_START_DATE = datetime.date(2020, 1, 1)

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