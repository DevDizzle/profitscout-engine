# transcript_collector/main.py
import logging
from google.cloud import storage
from config import PROJECT_ID, FMP_API_KEY_SECRET
from core.client import FMPClient
from core.orchestrator import run_pipeline

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

# Initialize clients globally for reuse to maintain connections across invocations
try:
    api_key = get_api_key_from_secret()
    storage_client = storage.Client(project=PROJECT_ID)
    fmp_client = FMPClient(api_key=api_key) if api_key else None
except Exception as e:
    logging.critical(f"A critical error occurred during client initialization: {e}")
    storage_client = fmp_client = None

def refresh_transcripts(request):
    """HTTP-triggered Google Cloud Function entry point."""
    if not all([storage_client, fmp_client]):
        logging.error("One or more clients are not initialized. Aborting.")
        return "Server configuration error: clients not initialized.", 500

    run_pipeline(fmp_client=fmp_client, storage_client=storage_client)
    return "Transcript collection pipeline started.", 202