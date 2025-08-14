# serving/functions/data_bundler/main.py

import logging
import functions_framework
from core import bundler

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@functions_framework.cloud_event
def run(cloud_event):
    """
    Main entry point. Orchestrates the final assembly of data from multiple sources
    and loads it into the destination BigQuery table.
    """
    logging.info("Data Bundler (Final Assembly) function triggered.")
    
    # 1. Get the base work list of tickers
    work_list_df = bundler.get_ticker_work_list_from_bq()
    if work_list_df.empty:
        logging.warning("No tickers in the work list. Shutting down.")
        return "No tickers to process."

    # 2. Get the calculated weighted scores
    scores_df = bundler.get_weighted_scores_from_bq()
    if scores_df.empty:
        logging.warning("No weighted scores found. Shutting down.")
        return "No scores to process."
        
    # 3. Assemble the final, complete metadata records
    final_metadata = bundler.assemble_final_metadata(work_list_df, scores_df)
    
    if not final_metadata:
        logging.warning("No complete records could be assembled. Nothing to load to BigQuery.")
        return "No complete records to process."
        
    # 4. Perform a single, final load to the destination BigQuery table
    try:
        bundler.replace_asset_metadata_in_bq(final_metadata)
    except Exception as e:
        logging.critical(f"CRITICAL: Final BigQuery load failed. Error: {e}")
        return "BigQuery load failed.", 500

    logging.info("Data Bundler (Final Assembly) job finished successfully.")
    return "OK", 200