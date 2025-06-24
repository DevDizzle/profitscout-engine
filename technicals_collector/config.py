# technicals_collector/config.py
import os

# --- GCP Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- API Keys ---
FMP_API_KEY_SECRET = os.getenv("FMP_API_KEY_SECRET", "FMP_API_KEY")

# --- Job Parameters ---
TICKER_LIST_PATH = "tickerlist.txt"
GCS_OUTPUT_FOLDER = "technicals/"
MAX_WORKERS = 6
DAYS_TO_KEEP = 90  # Number of recent days of technical data to store

# --- Indicators to Fetch ---
INDICATORS = {
    "sma_20": {"type": "sma", "period": 20},
    "ema_50": {"type": "ema", "period": 50},
    "rsi_14": {"type": "rsi", "period": 14},
    "adx_14": {"type": "adx", "period": 14},
    "obv":    {"type": "obv", "period": None},
    "macd":   {"type": "macd", "period": None},
    "stochastic": {"type": "stochastic", "period": None},
}