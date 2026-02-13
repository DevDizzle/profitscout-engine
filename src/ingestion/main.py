# src/ingestion/main.py
"""
Cloud Function entry points for the GammaRips ingestion pipelines.

This module defines HTTP and Pub/Sub-triggered functions that initiate various
data ingestion tasks. Each function is responsible for initializing the
necessary clients and invoking the corresponding pipeline from the `core.pipelines`
package.

The pipelines collect data from external sources like Financial Modeling Prep (FMP)
and SEC-API, persisting them to Google Cloud Storage and BigQuery.
"""

import logging
import os

import functions_framework
from flask import Request
from google.cloud import bigquery, firestore, pubsub_v1, storage

from .core import config
from .core.logger import setup_logging
from .core.clients.fmp_client import FMPClient
from .core.clients.polygon_client import PolygonClient
from .core.clients.sec_api_client import SecApiClient
from .core.pipelines import (
    calendar_events,
    fundamentals,
    history_archiver,  # Added import
    news_fetcher,
    options_chain_fetcher,
    populate_price_data,
    price_updater,
    refresh_stock_metadata,
    sec_filing_extractor,
    spy_price_sync,
    statement_loader,
    technicals_collector,
    transcript_collector,
)

# --- Global Initialization ---
setup_logging()


def _get_secret_or_env(name: str) -> str | None:
    """
    Retrieves a secret value, prioritizing environment variables.

    This function first checks for an environment variable with the given name.
    If not found, it attempts to read the secret from a mounted file at
    `/secrets/<NAME>`. This approach supports both local development (using
    environment variables) and Google Cloud's secret management.

    Args:
        name: The name of the secret (and environment variable) to retrieve.

    Returns:
        The secret value as a string, or None if not found.
    """
    if not name:
        return None
    val = os.environ.get(name)
    if val:
        return val
    try:
        with open(f"/secrets/{name}") as f:
            return f.read().strip()
    except (OSError, FileNotFoundError):
        logging.warning(f"Secret file not found for {name}.")
        return None


# --- Client Initialization ---
# Initialize Google Cloud clients.
storage_client = None
bq_client = None
publisher_client = None
firestore_client = None

if os.environ.get("ENV") != "test":
    storage_client = storage.Client(project=config.PROJECT_ID)
    bq_client = bigquery.Client(project=config.PROJECT_ID)
    publisher_client = pubsub_v1.PublisherClient()
    firestore_client = firestore.Client(project=config.DESTINATION_PROJECT_ID)

# Initialize API clients with keys from Secret Manager or environment.
fmp_api_key = _get_secret_or_env(config.FMP_API_KEY_SECRET)
sec_api_key = _get_secret_or_env(config.SEC_API_KEY_SECRET)
polygon_api_key = _get_secret_or_env(config.POLYGON_API_KEY_SECRET)

fmp_client = FMPClient(api_key=fmp_api_key) if fmp_api_key else None
sec_api_client = SecApiClient(api_key=sec_api_key) if sec_api_key else None
polygon_client = PolygonClient(api_key=polygon_api_key) if polygon_api_key else None


# --- Cloud Function Entry Points ---


@functions_framework.http
def refresh_fundamentals(request: Request):
    """
    HTTP-triggered function to run the fundamentals ingestion pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 202.
        Returns status 500 if clients are not initialized.
    """
    if not all([storage_client, fmp_client]):
        logging.error("Fundamentals clients not initialized.")
        return "Server config error: fundamentals clients not initialized.", 500
    fundamentals.run_pipeline(fmp_client=fmp_client, storage_client=storage_client)
    return "Fundamentals refresh pipeline started.", 202


@functions_framework.http
def update_prices(request: Request):
    """
    HTTP-triggered function to run the price updater pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 202.
        Returns status 500 if clients are not initialized.
    """
    if not all([storage_client, fmp_client]):
        logging.error("Price updater clients not initialized.")
        return "Server config error: price updater clients not initialized.", 500
    price_updater.run_pipeline(fmp_client=fmp_client, storage_client=storage_client)
    return "Price updater pipeline started.", 202


@functions_framework.http
def extract_sec_filings(request: Request):
    """
    HTTP-triggered function to run the SEC filing extractor pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 202.
        Returns status 500 if clients are not initialized.
    """
    if not all([storage_client, sec_api_client]):
        logging.error("SEC clients not initialized.")
        return "Server config error: SEC clients not initialized.", 500
    sec_filing_extractor.run_pipeline(
        client=sec_api_client, storage_client=storage_client
    )
    return "SEC filing extraction pipeline started.", 202


@functions_framework.http
def load_statements(request: Request):
    """
    HTTP-triggered function to run the statement loader pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 202.
        Returns status 500 if clients are not initialized.
    """
    if not all([storage_client, fmp_client]):
        logging.error("Statement loader clients not initialized.")
        return "Server config error: statement loader clients not initialized.", 500
    statement_loader.run_pipeline(fmp_client=fmp_client, storage_client=storage_client)
    return "Statement loader pipeline started.", 202


@functions_framework.http
def run_price_populator(request: Request):
    """
    HTTP-triggered function to run the price data populator pipeline.

    This pipeline backfills historical price data.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 202.
        Returns status 500 if clients are not initialized.
    """
    if not all([bq_client, storage_client, fmp_client]):
        logging.error("Price populator clients not initialized.")
        return "Server config error: price populator clients not initialized.", 500
    populate_price_data.run_pipeline(bq_client, storage_client, fmp_client)
    return "Price data population pipeline started.", 202


@functions_framework.http
def sync_spy_price_history(request: Request):
    """
    HTTP-triggered function to load SPY prices into BigQuery.
    """
    # We ONLY need BigQuery and FMP clients now.
    if not all([bq_client, fmp_client]):
        logging.error("SPY price sync clients not initialized.")
        return "Server config error: SPY price sync clients not initialized.", 500

    spy_price_sync.run_pipeline(
        bq_client=bq_client,
        fmp_client=fmp_client,
    )
    return "SPY price sync pipeline started.", 202


@functions_framework.http
def refresh_technicals(request: Request):
    """
    HTTP-triggered function to run the technicals collector pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 202.
        Returns status 500 if clients are not initialized.
    """
    if not all([storage_client, bq_client]):
        logging.error("Technicals collector clients not initialized.")
        return "Server config error: technicals collector clients not initialized.", 500
    technicals_collector.run_pipeline(storage_client, bq_client)
    return "Technicals collector pipeline started.", 202


@functions_framework.http
def refresh_stock_metadata_http(request: Request):
    """
    HTTP-triggered function to refresh stock metadata.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 200.
        Returns status 500 if clients are not initialized.
    """
    if not all([storage_client, bq_client, fmp_client, publisher_client]):
        logging.error("Metadata clients not initialized.")
        return "Server config error: metadata clients not initialized.", 500
    refresh_stock_metadata.run_pipeline(
        fmp_client, bq_client, storage_client, publisher_client
    )
    return "Stock metadata refresh pipeline finished.", 200


@functions_framework.http
def refresh_transcripts(request: Request):  # Change signature to accept 'request'
    """
    HTTP-triggered function to run the transcript collector pipeline.
    """
    # Ensure all clients are initialized (add polygon/sec if needed, though mostly just storage/fmp)
    if not all([storage_client, fmp_client]):
        logging.error("Transcript clients not initialized.")
        return "Server config error: transcript clients not initialized.", 500

    # Run the pipeline
    transcript_collector.run_pipeline(fmp_client, bq_client, storage_client)
    return "Transcript collection pipeline started.", 202


@functions_framework.http
def fetch_news(request: Request):
    """
    HTTP-triggered function to run the news fetcher pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 202.
    """
    # Note: news_fetcher does not need external clients passed in.
    news_fetcher.run_pipeline()
    return "News fetcher pipeline started.", 202


@functions_framework.http
def refresh_calendar_events(request: Request):
    """
    HTTP-triggered function to run the FMP calendar events pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 202.
        Returns status 500 if clients are not initialized.
    """
    if not all([bq_client, fmp_client, storage_client]):
        logging.error("Calendar events clients not initialized.")
        return "Server config error: calendar events clients not initialized.", 500
    calendar_events.run_pipeline(
        fmp_client=fmp_client, bq_client=bq_client, storage_client=storage_client
    )
    return "Calendar events pipeline started.", 202


@functions_framework.http
def fetch_options_chain(request: Request):
    """
    HTTP-triggered function to fetch options chain data.

    This function is triggered by an HTTP request. It fetches options chain data
    from the Polygon API and loads it into a BigQuery table.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a message and an HTTP status code.
    """
    try:
        _bq_client = bq_client or bigquery.Client(project=config.PROJECT_ID)
        _polygon_client = polygon_client or PolygonClient(
            api_key=_get_secret_or_env(config.POLYGON_API_KEY_SECRET)
        )

        if not all([_bq_client, _polygon_client]):
            logging.error("Options chain clients not initialized.")
            return "Server config error: options chain clients not initialized.", 500
        options_chain_fetcher.run_pipeline(
            polygon_client=_polygon_client, bq_client=_bq_client
        )

        # --- Archiver Step (Sidecar) ---
        # Persist today's snapshot to the history table for RL training.
        history_archiver.run_pipeline(bq_client=_bq_client)

        return "Options chain fetch started.", 202
    except ValueError as e:
        logging.error(f"Failed to initialize PolygonClient: {e}")
        return "Server config error: failed to initialize PolygonClient.", 500


@functions_framework.http
def run_history_archiver(request: Request):
    """
    HTTP-triggered function to run the history archiver pipeline.
    """
    if not bq_client:
        logging.error("History archiver clients not initialized.")
        return "Server config error: history archiver clients not initialized.", 500

    history_archiver.run_pipeline(bq_client=bq_client)
    return "History archiver pipeline started.", 202
