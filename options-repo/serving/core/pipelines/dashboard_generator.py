# serving/core/pipelines/dashboard_generator.py
import logging
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from typing import Dict, Any, Optional

from .. import config, gcs

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s")
PREP_PREFIX = "prep/"
OUTPUT_PREFIX = "dashboards/"
PRICE_CHART_JSON_FOLDER = "price-chart-json/"
MAX_WORKERS = 16 # Increased workers for a simpler, faster I/O bound task

def _delete_old_dashboard_files(ticker: str):
    """Deletes all previous dashboard JSON files for a given ticker."""
    prefix = f"{OUTPUT_PREFIX}{ticker}_dashboard_"
    blobs_to_delete = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix)
    for blob_name in blobs_to_delete:
        try:
            gcs.delete_blob(config.GCS_BUCKET_NAME, blob_name)
        except Exception as e:
            logging.error(f"[{ticker}] Failed to delete old dashboard file {blob_name}: {e}")

def _get_company_metadata(ticker: str) -> Dict[str, Any]:
    """Fetches basic company metadata from BigQuery."""
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    query = f"SELECT company_name FROM `{config.BUNDLER_STOCK_METADATA_TABLE_ID}` WHERE ticker = @ticker ORDER BY quarter_end_date DESC LIMIT 1"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)])
    df = client.query(query, job_config=job_config).to_dataframe()
    return df.iloc[0].to_dict() if not df.empty else {"company_name": ticker}

def _get_price_chart_data(ticker: str) -> Optional[Dict[str, Any]]:
    """Fetches the latest price chart JSON file for a ticker."""
    latest_blob = gcs.get_latest_blob_for_ticker(config.GCS_BUCKET_NAME, PRICE_CHART_JSON_FOLDER, ticker)
    if latest_blob:
        try:
            return json.loads(latest_blob.download_as_text())
        except (json.JSONDecodeError, Exception) as e:
            logging.error(f"[{ticker}] Failed to read or parse price chart JSON: {e}")
    return None

def process_prep_file(prep_blob_name: str) -> Optional[str]:
    """
    Processes a single prep file to generate a simple, data-only dashboard JSON.
    """
    match = re.search(r'prep/([A-Z\.]+)_(\d{4}-\d{2}-\d{2})\.json$', prep_blob_name)
    if not match:
        logging.warning(f"Could not parse ticker/date from prep file name: {prep_blob_name}. Skipping.")
        return None
    
    ticker, run_date_str = match.groups()
    logging.info(f"[{ticker}] Starting dashboard generation from {prep_blob_name}...")

    try:
        prep_json_str = gcs.read_blob(config.GCS_BUCKET_NAME, prep_blob_name)
        if not prep_json_str:
            logging.warning(f"[{ticker}] SKIPPING: Prep file is empty.")
            return None
        prep_data = json.loads(prep_json_str)

        if not prep_data.get("kpis"):
            logging.warning(f"[{ticker}] SKIPPING: Prep file is missing 'kpis' data.")
            return None

        metadata = _get_company_metadata(ticker)
        company_name = metadata.get("company_name", ticker)
        
        # Assemble the final dashboard with only the required data.
        final_dashboard = {
            "ticker": ticker,
            "runDate": run_date_str,
            "titleInfo": {"companyName": company_name, "ticker": ticker, "asOfDate": run_date_str},
            "kpis": prep_data.get("kpis"),
            "priceChartData": _get_price_chart_data(ticker),
        }
        
        _delete_old_dashboard_files(ticker)
        output_blob_name = f"{OUTPUT_PREFIX}{ticker}_dashboard_{run_date_str}.json"
        gcs.write_text(config.GCS_BUCKET_NAME, output_blob_name, json.dumps(final_dashboard, indent=2))
        logging.info(f"[{ticker}] SUCCESS: Generated and uploaded dashboard JSON.")
        return output_blob_name

    except Exception as e:
        logging.error(f"[{ticker}] CRITICAL ERROR during dashboard generation from {prep_blob_name}: {e}", exc_info=True)
        return None

def run_pipeline():
    """
    Main pipeline to generate a dashboard for every available prep file.
    """
    logging.info("--- Starting Dashboard Generation Pipeline (Ultra-Simplified) ---")
    
    work_items = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=PREP_PREFIX)
    if not work_items:
        logging.warning("No prep files found to process. Exiting.")
        return

    logging.info(f"Found {len(work_items)} prep files to process into dashboards.")
    processed_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_item = {executor.submit(process_prep_file, item): item for item in work_items}
        for future in as_completed(future_to_item):
            try:
                if future.result():
                    processed_count += 1
            except Exception as exc:
                logging.error(f"Prep file {future_to_item[future]} caused an unhandled exception: {exc}", exc_info=True)
    
    logging.info(f"--- Dashboard Generation Pipeline Finished. Successfully generated {processed_count} of {len(work_items)} dashboards. ---")

if __name__ == "__main__":
    run_pipeline()