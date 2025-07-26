#!/usr/bin/env python3
"""
Configuration for the Firestore Sync service.
"""

# BigQuery source table for the asset metadata
BQ_PROJECT_ID = "profitscout-c5po5"
BQ_DATASET = "profit_scout"
BQ_TABLE = "asset_metadata"

# Firestore destination collection
# The ticker symbol will be used as the Document ID.
FIRESTORE_COLLECTION = "tickers"