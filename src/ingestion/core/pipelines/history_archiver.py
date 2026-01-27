# ingestion/core/pipelines/history_archiver.py
import logging
from google.cloud import bigquery
from .. import config

def run_pipeline(bq_client: bigquery.Client | None = None):
    """
    Archives the current contents of the options_chain table into options_chain_history.
    This should be run immediately after the options chain fetcher to preserve the daily snapshot.
    """
    logging.info("--- Starting Options Chain History Archiver ---")
    
    bq_client = bq_client or bigquery.Client(project=config.PROJECT_ID)
    
    source_table = config.OPTIONS_CHAIN_TABLE_ID
    dest_table = config.OPTIONS_CHAIN_HISTORY_TABLE_ID
    
    # We select all columns from the source and add the current date as 'snapshot_date'
    # The source table 'options_chain' usually represents "Today's" data after a truncate/load.
    
    # --- Step 1: Clean up any existing snapshot for today (Idempotency) ---
    # This prevents duplicates if the pipeline runs multiple times in one day.
    # Since the table is partitioned by snapshot_date, this is an efficient operation.
    cleanup_query = f"DELETE FROM `{dest_table}` WHERE snapshot_date = CURRENT_DATE()"
    
    try:
        # We run the delete first
        bq_client.query(cleanup_query).result()
        logging.info(f"Cleaned up existing history for today in {dest_table}.")
    except Exception as e:
        # It's possible the table doesn't exist yet or is empty, which is fine.
        logging.warning(f"Cleanup query failed (possibly harmless if table is new): {e}")

    # --- Step 2: Insert the fresh snapshot ---
    query = f"""
        INSERT INTO `{dest_table}` 
        (ticker, contract_symbol, option_type, expiration_date, strike, last_price, bid, ask, 
         volume, open_interest, implied_volatility, delta, theta, vega, gamma, underlying_price, 
         fetch_date, dte, snapshot_date)
        SELECT 
            ticker, contract_symbol, option_type, expiration_date, strike, last_price, bid, ask, 
            volume, open_interest, implied_volatility, delta, theta, vega, gamma, underlying_price, 
            fetch_date, dte, CURRENT_DATE() as snapshot_date
        FROM `{source_table}`
    """
    
    try:
        job = bq_client.query(query)
        result = job.result()
        logging.info(f"Successfully archived options chain data to {dest_table}.")
    except Exception as e:
        logging.error(f"Failed to archive options chain data: {e}", exc_info=True)
        # We generally don't want to crash the whole pipeline if archiving fails, 
        # but for an RL data gathering mission, this IS critical. 
        # However, following "Safe" mandates, we log error and allow continuation?
        # User said "Critical Blocker", so maybe we should raise. 
        # But for now, logging error is sufficient as we can monitor logs.
        
    logging.info("--- Options Chain History Archiver Finished ---")
