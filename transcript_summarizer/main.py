import logging
from google.cloud import storage
from config import PROJECT_ID, GCS_BUCKET_NAME, GEMINI_API_KEY_SECRET_PATH
from core.client import GeminiClient
from core.orchestrator import run_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_api_key_from_secret() -> str | None:
    """Reads the API key from the file path mounted by Secret Manager."""
    secret_path = GEMINI_API_KEY_SECRET_PATH
    try:
        with open(secret_path, "r") as f:
            return f.read().strip()
    except Exception as e:
        logging.critical(f"CRITICAL: Could not read secret from {secret_path}: {e}")
        return None

def create_summaries(request):
    """HTTP-triggered Google Cloud Function entry point."""
    logging.info("Function triggered. Initializing clients.")
    
    try:
        # Securely get the API key from the mounted secret
        GEMINI_API_KEY = get_api_key_from_secret()
        if not all([PROJECT_ID, GCS_BUCKET_NAME, GEMINI_API_KEY]):
            logging.critical("Missing required environment variables or secrets.")
            return "Server configuration error", 500

        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        genai_client = GeminiClient(GEMINI_API_KEY)
        
        logging.info("Clients initialized successfully.")
    except Exception as e:
        logging.critical(f"Failed to initialize clients: {e}", exc_info=True)
        return "Client initialization failed", 500

    # Run the pipeline
    result = run_pipeline(genai_client, storage_client, bucket)
    return result, 200