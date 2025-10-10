# ingestion/core/config.py
import os
import datetime

# --- Global Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")

# --- API Key Secret Names ---
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# --- BigQuery ---
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")
# For Options
OPTIONS_CHAIN_TABLE = "options_chain"
OPTIONS_CHAIN_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{OPTIONS_CHAIN_TABLE}"