# populate_price_data/main.py
import logging
from google.cloud import bigquery, storage
from config import PROJECT_ID, FMP_API_KEY
from core.client import FMPClient
from core.orchestrator import run_pipeline

# Setup basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize clients globally to reuse connections
try:
    bq_client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)
    fmp_client = FMPClient(api_key=FMP_API_KEY)
except Exception as e:
    logging.critical(f"Failed to initialize clients: {e}")
    # Set clients to None so the function will fail gracefully
    bq_client = storage_client = fmp_client = None

def populate_price_data(request):
    """
    HTTP-triggered Google Cloud Function entry point.
    """
    if not all([bq_client, storage_client, fmp_client]):
        return "Server configuration error: clients not initialized.", 500

    run_pipeline(bq_client, storage_client, fmp_client)
    return "Pipeline execution started.", 202