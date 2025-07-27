#!/usr/bin/env python3
"""
Cloud Function entry point for the Data Bundler service.
This function is triggered by a Pub/Sub message, typically from Cloud Scheduler.
It fetches a list of tickers, and for each ticker, it creates and uploads
a data bundle to Google Cloud Storage using multiple threads.
"""
import functions_framework
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from core.bundler import create_and_upload_bundle, get_ticker_list
from config import BUCKET_NAME

# --- Client Initialization ---
# Initialize clients once globally to be reused across function invocations.
try:
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
except Exception as e:
    print(f"[FATAL] Could not initialize GCS client: {e}")
    bucket = None

# --- Configuration ---
MAX_WORKERS = 4

def process_single_ticker(ticker: str):
    """
    Wrapper function to process one ticker and handle its exceptions.
    This is the target function for each thread.
    """
    try:
        print(f"--- Starting processing for ticker: {ticker} ---")
        # The bucket object is passed from the global scope
        create_and_upload_bundle(bucket, ticker)
        return f"SUCCESS: {ticker}"
    except Exception as e:
        # Log the error but don't crash the entire process.
        print(f"[ERROR] An unexpected error occurred while processing {ticker}: {e}")
        return f"ERROR: {ticker} - {e}"

@functions_framework.cloud_event
def run(cloud_event):
    """
    Main function triggered by Pub/Sub.
    Processes all tickers from the ticker list file in GCS using a thread pool.
    """
    start_time = time.time()
    print("[INFO] Data Bundler function triggered.")

    if not bucket:
        print("[FATAL] GCS bucket is not initialized. Exiting.")
        return "GCS client not initialized.", 500

    tickers = get_ticker_list(bucket)
    if not tickers:
        print("[ERROR] No tickers found. Exiting function.")
        return "No tickers processed.", 200

    print(f"[INFO] Found {len(tickers)} tickers. Starting processing with {MAX_WORKERS} workers.")

    results = []
    # Using ThreadPoolExecutor to process tickers in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Create a future for each ticker
        future_to_ticker = {executor.submit(process_single_ticker, ticker): ticker for ticker in tickers}

        # As each future completes, process the result
        for future in as_completed(future_to_ticker):
            result = future.result()
            print(f"[COMPLETED] {result}")
            results.append(result)

    end_time = time.time()
    print(f"[SUCCESS] Finished processing all {len(tickers)} tickers in {end_time - start_time:.2f} seconds.")
    return "OK", 200