# src/ingestion/main.py
"""
Cloud Function entry points for the ProfitScout ingestion pipelines.

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
from google.cloud import bigquery, pubsub_v1, storage

from .core import config
from .core.clients.fmp_client import FMPClient
from .core.clients.sec_api_client import SecApiClient
from .core.pipelines import (
    calendar_events,
    fundamentals,
    news_fetcher,
    populate_price_data,
    price_updater,
    refresh_stock_metadata,
    sec_filing_extractor,
    statement_loader,
    technicals_collector,
    transcript_collector,
)

# --- Global Initialization ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


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
    val = os.environ.get(name)
    if val:
        return val
    try:
        with open(f"/secrets/{name}", "r") as f:
            return f.read().strip()
    except (FileNotFoundError, IOError):
        logging.warning(f"Secret file not found for {name}.")
        return None


# --- Client Initialization ---
# Initialize Google Cloud clients.
storage_client = storage.Client(project=config.PROJECT_ID)
bq_client = bigquery.Client(project=config.PROJECT_ID)
publisher_client = pubsub_v1.PublisherClient()

# Initialize API clients with keys from Secret Manager or environment.
fmp_api_key = _get_secret_or_env(config.FMP_API_KEY_SECRET)
sec_api_key = _get_secret_or_env(config.SEC_API_KEY_SECRET)

fmp_client = FMPClient(api_key=fmp_api_key) if fmp_api_key else None
sec_api_client = SecApiClient(api_key=sec_api_key) if sec_api_key else None


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


@functions_framework.cloud_event
def refresh_transcripts(cloud_event):
    """
    Pub/Sub-triggered function to run the transcript collector pipeline.

    Args:
        cloud_event: The CloudEvent object.

    Returns:
        A tuple containing a success message and HTTP status code 200.

    Raises:
        ConnectionError: If required clients are not initialized.
    """
    if not all([storage_client, bq_client, fmp_client]):
        logging.error("Transcript clients not initialized.")
        raise ConnectionError("Server config error: transcript clients not initialized.")
    transcript_collector.run_pipeline(fmp_client, bq_client, storage_client)
    return "Transcript collection pipeline finished successfully.", 200


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