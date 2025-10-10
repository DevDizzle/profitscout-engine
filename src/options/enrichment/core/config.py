# enrichment/core/config.py

import os

# --- Global Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")

# --- BigQuery ---
SCORES_TABLE = f"{PROJECT_ID}.{BIGQUERY_DATASET}.analysis_scores"
CHAIN_TABLE = f"{PROJECT_ID}.{BIGQUERY_DATASET}.options_chain"
CAND_TABLE = f"{PROJECT_ID}.{BIGQUERY_DATASET}.options_candidates"
PRICE_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.price_data"

# --- Vertex AI (Shared) ---
MODEL_NAME = os.environ.get("MODEL_NAME", "gemini-2.5-pro")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.7"))
TOP_P = float(os.getenv("TOP_P", "0.95"))
TOP_K = int(os.getenv("TOP_K", "40"))
SEED = int(os.getenv("SEED", "42"))
CANDIDATE_COUNT = int(os.getenv("CANDIDATE_COUNT", "1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "2048"))
