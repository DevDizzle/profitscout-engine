# ingestion/main.py
import logging
import functions_framework
from google.cloud import storage, bigquery, pubsub_v1
from core import config
from core.clients.fmp_client import FMPClient
from core.clients.sec_api_client import SecApiClient
from core.pipelines import (
    fundamentals,
    price_updater,
    sec_filing_extractor,
    statement_loader,
    populate_price_data,
    technicals_collector,
    transcript_collector,
    refresh_stock_metadata,
)

# --- Global Initialization (Shared Across Functions) ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def _get_api_key(secret_name: str) -> str | None:
    """Helper to read any secret from the mounted path."""
    secret_path = f"/secrets/{secret_name}"
    try:
        with open(secret_path, "r") as f:
            return f.read().strip()
    except (IOError, FileNotFoundError) as e:
        logging.critical(f"Could not read secret '{secret_name}': {e}")
        return None

# Initialize clients once for all functions to use
storage_client = storage.Client(project=config.PROJECT_ID)
bq_client = bigquery.Client(project=config.PROJECT_ID)
publisher_client = pubsub_v1.PublisherClient()

fmp_api_key = _get_api_key(config.FMP_API_KEY_SECRET)
sec_api_key = _get_api_key(config.SEC_API_KEY_SECRET)

fmp_client = FMPClient(api_key=fmp_api_key) if fmp_api_key else None
sec_api_client = SecApiClient(api_key=sec_api_key) if sec_api_key else None

# --- Cloud Function Entry Points ---

@functions_framework.http
def refresh_fundamentals(request):
    """Entry point for the fundamentals pipeline."""
    if not all([storage_client, fmp_client]):
        return "Server config error: fundamentals clients not initialized.", 500
    fundamentals.run_pipeline(fmp_client=fmp_client, storage_client=storage_client)
    return "Fundamentals refresh pipeline started.", 202

@functions_framework.http
def update_prices(request):
    """Entry point for the price updater pipeline."""
    if not all([storage_client, fmp_client]):
        return "Server config error: price updater clients not initialized.", 500
    price_updater.run_pipeline(fmp_client=fmp_client, storage_client=storage_client)
    return "Price updater pipeline started.", 202

@functions_framework.http
def extract_sec_filings(request):
    """Entry point for the SEC filing extractor pipeline."""
    if not all([storage_client, sec_api_client]):
        return "Server config error: SEC clients not initialized.", 500
    sec_filing_extractor.run_pipeline(client=sec_api_client, storage_client=storage_client)
    return "SEC filing extraction pipeline started.", 202

@functions_framework.http
def load_statements(request):
    """Entry point for the statement loader pipeline."""
    if not all([storage_client, fmp_client]):
        return "Server config error: statement loader clients not initialized.", 500
    statement_loader.run_pipeline(fmp_client=fmp_client, storage_client=storage_client)
    return "Statement loader pipeline started.", 202

@functions_framework.http
def populate_price_data(request):
    """Entry point for the price data populator pipeline."""
    if not all([bq_client, storage_client, fmp_client]):
        return "Server config error: price populator clients not initialized.", 500
    populate_price_data.run_pipeline(bq_client, storage_client, fmp_client)
    return "Price data population pipeline started.", 202

@functions_framework.http
def refresh_technicals(request):
    """Entry point for the technicals collector pipeline."""
    if not all([storage_client, bq_client]):
         return "Server config error: technicals collector clients not initialized.", 500
    technicals_collector.run_pipeline(storage_client, bq_client)
    return "Technicals collector pipeline started.", 202
    
@functions_framework.http
def refresh_stock_metadata_http(request):
    """HTTP-triggered entry point for stock metadata refresh."""
    if not all([storage_client, bq_client, fmp_client, publisher_client]):
        return "Server config error: metadata clients not initialized.", 500
    refresh_stock_metadata.run_pipeline(fmp_client, bq_client, storage_client, publisher_client)
    return "Stock metadata refresh pipeline finished.", 200

@functions_framework.cloud_event
def refresh_transcripts(cloud_event):
    """Pub/Sub-triggered entry point for the transcript collector."""
    if not all([storage_client, bq_client, fmp_client]):
        raise ConnectionError("Server config error: transcript clients not initialized.")
    transcript_collector.run_pipeline(fmp_client, bq_client, storage_client)
    return "Transcript collection pipeline finished successfully.", 200