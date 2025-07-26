#!/usr/bin/env python3
"""
Configuration for the Data Bundler service.
"""
from typing import Dict

# GCS bucket where the source data pipeline artifacts are stored.
BUCKET_NAME = "profit-scout-data"
TICKER_LIST_PATH = "tickerlist.txt"

# GCS destination for the final JSON bundles.
BUNDLE_OUTPUT_BUCKET = "profit-scout-data-store"
BUNDLE_OUTPUT_FOLDER = "bundles"

# --- BigQuery Configuration ---

# Source project and table for stock metadata
SOURCE_BQ_PROJECT = "profitscout-lx6bb"
SOURCE_BQ_DATASET = "profit_scout"
SOURCE_BQ_TABLE = "stock_metadata"

# Destination project and table for the final asset metadata
DESTINATION_BQ_PROJECT = "profitscout-c5po5"
DESTINATION_BQ_DATASET = "profit_scout"
DESTINATION_BQ_TABLE = "asset_metadata"


# --- GCS Path Templates ---
# The '{t}' placeholder will be replaced with the ticker symbol.
SECTION_GCS_PATHS: Dict[str, str] = {
    "earnings-call-transcripts": "earnings-call-transcripts/",
    "sec-business": "sec-business/{t}_business_profile.json",
    "sec-mda": "sec-mda/",
    "financial-statements": "financial-statements/",
    "key-metrics": "key-metrics/",
    "ratios": "ratios/",
    "technicals": "technicals/{t}_technicals.json",
    "prices": "prices/{t}_90_day_prices.json",
}