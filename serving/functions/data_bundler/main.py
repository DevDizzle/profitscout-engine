# data_bundler/main.py

import logging
import functions_framework
from concurrent.futures import ThreadPoolExecutor
from core import bundler  # <-- CORRECTED IMPORT

# --- Configuration ---
MAX_WORKERS = 16 # Increased for I/O-bound tasks

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@functions_framework.cloud_event
def run(cloud_event):
    """
    Main entry point for the Cloud Function.
    Orchestrates the entire data bundling process.
    """
    logging.info("Data Bundler function triggered.")
    
    # 1. Get the master work list from BigQuery
    work_list = bundler.get_ticker_work_list_from_bq()
    if not work_list:
        logging.warning("No tickers to process. Shutting down.")
        return "No tickers to process."

    # 2. Process tickers in parallel using a thread pool
    logging.info(f"Starting parallel processing for {len(work_list)} tickers with {MAX_WORKERS} workers.")
    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # The map function will automatically handle the iteration and collect results
        all_results = list(executor.map(bundler.create_and_upload_bundle, work_list))

    # 3. Filter out any failed tasks (which return None)
    successful_metadata = [res for res in all_results if res is not None]
    
    total_tasks = len(work_list)
    successful_tasks = len(successful_metadata)
    failed_tasks = total_tasks - successful_tasks
    logging.info(f"Processing complete. Success: {successful_tasks}, Failed: {failed_tasks}")
    
    # 4. Perform a single batch update to BigQuery
    if successful_metadata:
        try:
            bundler.batch_update_asset_metadata_in_bq(successful_metadata)
        except Exception as e:
            # If the BQ update fails, we stop to avoid sending a false "success" signal
            logging.critical(f"CRITICAL: BigQuery batch update failed. Downstream processes will not be triggered. Error: {e}")
            return "BigQuery batch update failed.", 500
    else:
        logging.warning("No bundles were created, so no BigQuery update is necessary.")

    # 5. Publish a single "job complete" message
    bundler.publish_job_complete_message()
    
    logging.info("Data Bundler job finished successfully.")
    return "OK", 200