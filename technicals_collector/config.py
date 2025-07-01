# technicals_collector/config.py
import os

# --- GCP Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- BigQuery ---
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "price_data")
BIGQUERY_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

# --- Job Parameters ---
TICKER_LIST_PATH = "tickerlist.txt"
GCS_OUTPUT_FOLDER = "technicals/"
MAX_WORKERS = 16
ROLLING_52_WEEK_WINDOW = 252

# --- Indicator Definitions ---
INDICATORS = {
    # Trend
    "sma_50": {"kind": "sma", "params": {"length": 50}},
    "sma_200": {"kind": "sma", "params": {"length": 200}},
    "ema_21": {"kind": "ema", "params": {"length": 21}},
    "adx": {"kind": "adx", "params": {"length": 14}},
    # Momentum
    "macd": {"kind": "macd", "params": {"fast": 12, "slow": 26, "signal": 9}},
    "rsi_14": {"kind": "rsi", "params": {"length": 14}},
    "stochastic": {"kind": "stoch", "params": {"k": 14, "d": 3, "smooth_k": 3}},
    "roc_20": {"kind": "roc", "params": {"length": 20}},
    # Volatility
    "bollinger_bands": {"kind": "bbands", "params": {"length": 20, "std": 2}},
    "atr": {"kind": "atr", "params": {"length": 14}},
    # Volume
    "obv": {"kind": "obv", "params": {}},
}