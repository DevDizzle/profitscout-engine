# ingestion/core/orchestrators/populate_price_data.py
import logging
import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage, bigquery
from .. import config, bq
from ..gcs import get_tickers
from ..clients.fmp_client import FMPClient

def run_pipeline(bq_client: bigquery.Client, storage_client: storage.Client, fmp_client: FMPClient):
    logging.info("=== Starting Price Population Pipeline ===")
    all_tickers = get_tickers(storage_client)
    if not all_tickers:
        logging.warning("No tickers found. Exiting.")
        return

    total_rows_loaded = 0
    today = datetime.date.today()
    max_workers = config.MAX_WORKERS_TIERING.get("populate_price_data")

    for i in range(0, len(all_tickers), config.BATCH_SIZE):
        batch_tickers = all_tickers[i:i + config.BATCH_SIZE]
        logging.info(f"--- Processing Price Populator Batch {i//config.BATCH_SIZE + 1} ---")

        start_dates = bq.get_start_dates_for_populator(bq_client, batch_tickers)
        batch_dfs = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_ticker = {
                executor.submit(fmp_client.fetch_prices_for_populator, t, start_dates.get(t, today), today): t
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
        rows_loaded = bq.load_data_to_bigquery(bq_client, final_df)
        total_rows_loaded += rows_loaded

    logging.info(f"=== Price Populator Pipeline complete. Total rows loaded: {total_rows_loaded} ===")