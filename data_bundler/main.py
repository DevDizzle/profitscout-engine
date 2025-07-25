#!/usr/bin/env python3
"""
Cloud Function Entry Point for the Data Bundler Service.

Triggered by a Pub/Sub message, this function reads a list of tickers,
and for each one, it collects all its related data artifacts from GCS,
combines them into a single JSON file, and uploads it back to a 'bundles'
folder in the same GCS bucket.
"""
import functions_framework
from google.cloud import storage
from cloudevents.http import CloudEvent

from config import BUCKET_NAME
from core.bundler import create_and_upload_bundle, get_ticker_list

@functions_framework.cloud_event
def run(cloud_event: CloudEvent) -> None:
    """
    Main entry point for the Cloud Function.
    Args:
        cloud_event: The CloudEvent that triggered this function.
    """
    print("Data Bundler service triggered.")

    # Initialize the GCS client and get the bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # Get the list of tickers to process
    tickers = get_ticker_list(bucket)
    if not tickers:
        print("[ERROR] Ticker list is empty. Exiting function.")
        return

    # Process each ticker
    for ticker in tickers:
        try:
            create_and_upload_bundle(bucket, ticker)
        except Exception as e:
            print(f"[FATAL] An unexpected error occurred while processing {ticker}: {e}")

    print("Data Bundler service finished.")