#!/usr/bin/env python3
"""
Local test script for the Data Bundler Service.

This script allows you to run the data bundling logic on your local machine
to test it before deploying it as a Cloud Function.
"""
from google.cloud import storage

# Make sure the 'data_bundler' directory is on the Python path
# so we can import from 'core' and 'config'.
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_bundler.config import BUCKET_NAME
from data_bundler.core.bundler import create_and_upload_bundle

# --- For Testing Only ---
# Set the specific ticker you want to test.
TEST_TICKER = "AAL"
# ------------------------

def main():
    """
    Main function for local testing.
    """
    print("--- Starting Data Bundler Local Test ---")
    
    if not TEST_TICKER:
        print("[ERROR] TEST_TICKER variable is not set. Exiting.")
        return

    try:
        # Authenticate and get the GCS bucket
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        print(f"--- RUNNING IN TEST MODE FOR TICKER: {TEST_TICKER} ---")
        
        # Call the core bundling logic directly
        create_and_upload_bundle(bucket, TEST_TICKER)

    except Exception as e:
        print(f"[FATAL] An unexpected error occurred: {e}")

    print("\n--- Data Bundler Local Test Finished ---")

# This is the crucial part:
# It tells Python to run the main() function when you execute the script.
if __name__ == "__main__":
    main()