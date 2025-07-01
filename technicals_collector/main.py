# technicals_collector/main.py
import logging
from google.cloud import storage, bigquery
from config import PROJECT_ID
from core.orchestrator import run_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

try:
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
except Exception as e:
    logging.critical(f"Failed to initialize Google Cloud clients: {e}")
    storage_client = None
    bq_client = None

def refresh_technicals(request):
    """
    HTTP-triggered Google Cloud Function entry point for the refactored pipeline.
    """
    logging.info("--- Technicals Collector Function Started ---")
    
    if not storage_client or not bq_client:
        logging.error("One or more Google Cloud clients are not initialized. Aborting.")
        return "Server configuration error: clients not initialized.", 500

    try:
        logging.info("Starting pipeline execution...")
        run_pipeline(storage_client=storage_client, bq_client=bq_client)
        
        logging.info("--- Technicals Collector Function Finished Successfully ---")
        return "Technicals collection pipeline finished.", 200 # Return 200 OK on success
        
    except Exception as e:
        logging.critical(f"A critical unhandled exception occurred in the main handler: {e}", exc_info=True)
        return f"An unexpected error occurred: {e}", 500