# technicals_collector/core/orchestrator.py
import logging
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import MAX_WORKERS, GCS_BUCKET_NAME, GCS_OUTPUT_FOLDER, INDICATORS
from core.gcs import get_tickers, upload_json_to_gcs
from core.client import FMPClient
from google.cloud import storage

def process_ticker(ticker: str, fmp_client: FMPClient, storage_client: storage.Client):
    """Fetches all technical indicators for a ticker and saves them to a single JSON."""
    output = {
        "ticker": ticker,
        "as_of": datetime.date.today().isoformat(),
        "technicals": {}
    }

    for label, config in INDICATORS.items():
        data = fmp_client.fetch_indicator(ticker, config["type"], config.get("period"))
        if not data:
            logging.warning(f"{ticker}: No data for indicator '{label}'.")
            continue

        # Handle special formatting for certain indicators
        if config["type"] == "macd":
            output["technicals"]["macd"] = [
                {"date": d["date"], "line": d.get("macd"), "signal": d.get("macdSignal"), "histogram": d.get("macdHist")}
                for d in data
            ]
        elif config["type"] == "stochastic":
            output["technicals"]["stochastic"] = [
                {"date": d["date"], "k": d.get("stochK"), "d": d.get("stochD")}
                for d in data
            ]
        else:
            output["technicals"][label] = [
                {"date": d["date"], "value": d.get(config["type"])}
                for d in data
            ]

    if not output["technicals"]:
        return f"{ticker}: No technical data found, file not created."

    # Use the new flattened path
    blob_path = f"{GCS_OUTPUT_FOLDER}{ticker}_technicals.json"
    upload_json_to_gcs(storage_client, GCS_BUCKET_NAME, output, blob_path)
    return f"{ticker}: Technicals JSON uploaded to {blob_path}."

def run_pipeline(fmp_client: FMPClient, storage_client: storage.Client):
    """Runs the full technicals collection pipeline."""
    tickers = get_tickers(storage_client, GCS_BUCKET_NAME)
    if not tickers:
        logging.error("No tickers found. Exiting.")
        return

    logging.info(f"Starting technicals collection for {len(tickers)} tickers.")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ticker, t, fmp_client, storage_client): t for t in tickers}
        for future in as_completed(futures):
            try:
                result = future.result()
                logging.info(result)
            except Exception as e:
                ticker = futures[future]
                logging.error(f"{ticker}: An error occurred: {e}", exc_info=True)

    logging.info("Technicals collection pipeline complete.")