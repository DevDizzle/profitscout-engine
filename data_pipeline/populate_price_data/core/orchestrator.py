import logging
import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage, bigquery
from config import MAX_WORKERS, BATCH_SIZE
from core.gcs import get_tickers_from_gcs
from core.bq import get_start_dates, load_data_to_bigquery
from core.client import FMPClient

def run_pipeline(bq_client: bigquery.Client, storage_client: storage.Client, fmp_client: FMPClient):
    """Executes the full price data pipeline."""
    logging.info("=== Starting Price Update Pipeline ===")
    all_tickers = get_tickers_from_gcs(storage_client)
    if not all_tickers:
        logging.warning("No tickers found. Exiting.")
        return

    total_rows_loaded = 0
    today = datetime.date.today()

    # Process tickers in batches to manage memory and API calls
    for i in range(0, len(all_tickers), BATCH_SIZE):
        batch_tickers = all_tickers[i:i + BATCH_SIZE]
        logging.info(f"--- Processing batch {i//BATCH_SIZE + 1}/{len(all_tickers)//BATCH_SIZE + 1} ---")

        start_dates = get_start_dates(bq_client, batch_tickers)
        batch_dfs = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_ticker = {
                executor.submit(fmp_client.fetch_prices, t, start_dates.get(t, today), today): t
                for t in batch_tickers
            }
            for future in as_completed(future_to_ticker):
                df = future.result()
                if not df.empty:
                    batch_dfs.append(df)

        if not batch_dfs:
            logging.info("No new price data in this batch.")
            continue

        final_df = pd.concat(batch_dfs, ignore_index=True)
        rows_loaded = load_data_to_bigquery(bq_client, final_df)
        total_rows_loaded += rows_loaded

    logging.info(f"=== Pipeline complete. Total rows loaded: {total_rows_loaded} ===")