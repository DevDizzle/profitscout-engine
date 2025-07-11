import logging
import google.cloud.logging # Import the client library
from google.cloud import storage, bigquery, pubsub_v1

from . import config
from .core.client import FMPClient
from .core.orchestrator import run_pipeline

# --- NEW: More robust logging setup ---
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()
# ------------------------------------

def get_api_key_from_secret() -> str | None:
    """Reads the API key from the file path mounted by Secret Manager."""
    secret_path = f"/secrets/{config.FMP_API_KEY_SECRET}"
    try:
        with open(secret_path, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        logging.critical(f"Secret not found at {secret_path}. Ensure it is mounted correctly.")
        return None
    except IOError as e:
        logging.critical(f"Could not read secret from {secret_path}: {e}")
        return None

# Initialize clients globally for reuse
try:
    api_key = get_api_key_from_secret()
    storage_client = storage.Client(project=config.PROJECT_ID)
    bq_client = bigquery.Client(project=config.PROJECT_ID)
    publisher_client = pubsub_v1.PublisherClient()
    fmp_client = FMPClient(api_key=api_key) if api_key else None
except Exception as e:
    logging.critical(f"A critical error occurred during client initialization: {e}", exc_info=True)
    storage_client = bq_client = publisher_client = fmp_client = None

def refresh_transcripts(event, context):
    """
    Pub/Sub-triggered Google Cloud Function entry point.
    """
    logging.info("Transcript collector function triggered by Pub/Sub message.")

    if not all([storage_client, bq_client, publisher_client, fmp_client]):
        logging.error("One or more clients are not initialized. Aborting. Check for critical errors during startup.")
        # This is a server-side configuration error, so we raise an exception.
        raise ConnectionError("Server configuration error: clients not initialized.")

    try:
        run_pipeline(
            fmp_client=fmp_client, 
            bq_client=bq_client, 
            storage_client=storage_client,
            publisher_client=publisher_client
        )
        return "Transcript collection pipeline finished successfully.", 200
    except Exception as e:
        logging.critical(f"An unhandled exception occurred in the pipeline: {e}", exc_info=True)
        raise