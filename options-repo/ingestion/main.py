# ingestion/main.py
import logging
import os
import functions_framework
from google.cloud import bigquery

from core import config
from core.clients.polygon import PolygonClient
from core.pipelines import options_chain_fetcher

# --- Global Initialization (Shared Across Functions) ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

bq_client = bigquery.Client(project=config.PROJECT_ID)

@functions_framework.http
def fetch_options_chain(request):
    """
    Entry point for the options chain fetcher.
    Uses Polygon snapshot API to retrieve ≤90d options chains for top/bottom 10 tickers,
    then loads normalized rows into BigQuery profit_scout.options_chain.
    """
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        return "POLYGON_API_KEY not set", 500

    if not bq_client:
        return "Server config error: BigQuery client not initialized.", 500

    polygon_client = PolygonClient(api_key=api_key)
    options_chain_fetcher.run_pipeline(polygon_client=polygon_client, bq_client=bq_client)
    return "Options chain fetch started.", 202