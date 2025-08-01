import logging
from google.cloud import bigquery, storage
from config import PROJECT_ID, FMP_API_KEY_SECRET
from core.client import FMPClient
from core.orchestrator import run_pipeline

# Setup basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_api_key_from_secret() -> str | None:
    """Reads the API key from the file path mounted by Secret Manager."""
    secret_path = f"/secrets/{FMP_API_KEY_SECRET}"
    try:
        with open(secret_path, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        logging.critical(f"Secret not found at {secret_path}. Ensure it is mounted correctly.")
        return None
    except IOError as e:
        logging.critical(f"Could not read secret from {secret_path}: {e}")
        return None

# Initialize clients globally to reuse connections
try:
    api_key = get_api_key_from_secret()
    bq_client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)
    fmp_client = FMPClient(api_key=api_key) if api_key else None
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