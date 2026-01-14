
import logging
import os
import json
from google.cloud import storage
from src.ingestion.core.clients.polygon_client import PolygonClient
from src.ingestion.core.pipelines import news_fetcher
from src.enrichment.core.pipelines import news_analyzer
from src.enrichment.core import gcs, config

# Setup
logging.basicConfig(level=logging.INFO)
storage_client = storage.Client()
polygon_client = PolygonClient(api_key=os.getenv("POLYGON_API_KEY"))
TICKER = "LRCX"

print(f"--- TESTING NEWS PIPELINE FOR {TICKER} ---")

# 1. RUN FETCHER
print(f"\n[1] Running Fetcher for {TICKER}...")
try:
    news_fetcher.fetch_and_save(TICKER, polygon_client, storage_client)
    print("Fetcher finished.")
except Exception as e:
    print(f"Fetcher failed: {e}")

# 2. READ FETCHED INPUT
input_prefix = config.PREFIXES["news_analyzer"]["input"]
blobs = list(storage_client.list_blobs(config.GCS_BUCKET_NAME, prefix=f"{input_prefix}{TICKER}_"))
if not blobs:
    print("No input file created!")
    exit(1)

latest_blob = sorted(blobs, key=lambda x: x.updated, reverse=True)[0]
print(f"Reading input from: {latest_blob.name}")
input_content = latest_blob.download_as_text()
print(f"Input JSON Summary: {input_content[:500]}...") # Show first 500 chars

# 3. RUN ANALYZER
print(f"\n[2] Running Analyzer for {TICKER}...")
try:
    # We call process_blob directly
    output_path = news_analyzer.process_blob(latest_blob.name, storage_client)
    print(f"Analyzer finished. Output at: {output_path}")
    
    if output_path:
        # 4. READ ANALYZER OUTPUT
        out_blob = storage_client.bucket(config.GCS_BUCKET_NAME).blob(output_path)
        print(f"\n[3] FINAL ANALYSIS for {TICKER}:")
        print(out_blob.download_as_text())
        
except Exception as e:
    print(f"Analyzer failed: {e}")
