# ingestion/main.py
import logging
import os
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
    news_fetcher,
    calendar_events,
)

# --- Global Initialization (Shared Across Functions) ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def _get_secret_or_env(name: str) -> str | None:
    """
    Prefer environment variables (works with --set-secrets). Fall back to
    reading /secrets/<NAME> only if mounted. Return None if not available.
    """
    val = os.environ.get(name)
    if val:
        return val
    try:
        with open(f"/secrets/{name}", "r") as f:
            return f.read().strip()
    except Exception:
        return None

# Initialize shared clients once
storage_client = storage.Client(project=config.PROJECT_ID)
bq_client = bigquery.Client(project=config.PROJECT_ID)
publisher_client = pubsub_v1.PublisherClient()

# Optional secrets for existing pipelines (don't crash if missing)
fmp_api_key = _get_secret_or_env(config.FMP_API_KEY_SECRET)
sec_api_key = _get_secret_or_env(config.SEC_API_KEY_SECRET)

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
    populate_price_data_pipeline.run_pipeline(bq_client, storage_client, fmp_client)
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

@functions_framework.http
def fetch_news(request):
    """Entry point for the news fetcher pipeline."""
    # Note: news_fetcher does not need external clients passed in
    news_fetcher.run_pipeline()
    return "News fetcher pipeline started.", 202

@functions_framework.http
def refresh_calendar_events(request):
    """Entry point for the FMP calendar events pipeline."""
    if not all([bq_client, fmp_client, storage_client]):
        return "Server config error: calendar events clients not initialized.", 500
    calendar_events.run_pipeline(
        fmp_client=fmp_client, 
        bq_client=bq_client, 
        storage_client=storage_client
    )
    return "Calendar events pipeline started.", 202