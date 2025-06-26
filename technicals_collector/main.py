# technicals_collector/main.py
import logging
from google.cloud import storage
from config import PROJECT_ID
from core.orchestrator import run_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize clients globally for reuse
try:
    storage_client = storage.Client(project=PROJECT_ID)
except Exception as e:
    logging.critical(f"Failed to initialize Storage client: {e}")
    storage_client = None

def refresh_technicals(request):
    """
    HTTP-triggered Google Cloud Function entry point for the refactored pipeline.
    """
    if not storage_client:
        logging.error("Storage client is not initialized. Aborting.")
        return "Server configuration error: clients not initialized.", 500

    # The orchestrator now only needs the storage_client, as the BigQuery client
    # is initialized within the orchestrator module itself.
    run_pipeline(storage_client=storage_client)
    
    return "Technicals collection pipeline (refactored) started.", 202