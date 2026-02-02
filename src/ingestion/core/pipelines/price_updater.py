# ingestion/core/orchestrators/price_updater.py
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage

from .. import config
from ..clients.fmp_client import FMPClient
from ..gcs import get_tickers, upload_json_to_gcs


def process_ticker(ticker: str, fmp_client: FMPClient, storage_client: storage.Client):
    price_records = fmp_client.fetch_90_day_prices(ticker)
    if not price_records:
        return f"{ticker}: No price data returned from API."

    output_doc = {
        "ticker": ticker,
        "as_of_date": datetime.date.today().isoformat(),
        "prices": price_records,
    }
    blob_path = f"{config.PRICE_UPDATER_OUTPUT_FOLDER}{ticker}_90_day_prices.json"
    upload_json_to_gcs(storage_client, output_doc, blob_path)
    return f"{ticker}: Price snapshot uploaded successfully."


def run_pipeline(fmp_client: FMPClient, storage_client: storage.Client):
    tickers = get_tickers(storage_client)
    if not tickers:
        logging.error("No tickers found. Exiting price updater pipeline.")
        return

    logging.info(f"Starting 90-day price update for {len(tickers)} tickers.")
    max_workers = config.MAX_WORKERS_TIERING.get("price_updater")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_ticker, t, fmp_client, storage_client): t
            for t in tickers
        }
        for future in as_completed(futures):
            try:
                logging.info(future.result())
            except Exception as e:
                logging.error(
                    f"'{futures[future]}': An error occurred: {e}", exc_info=True
                )
    logging.info("Price updater pipeline complete.")
