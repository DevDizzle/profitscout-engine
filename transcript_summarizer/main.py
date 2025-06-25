# transcript_summarizer/main.py
import logging
from google.cloud import storage
from config import PROJECT_ID
from core.client import GeminiClient
from core.orchestrator import run_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize clients globally. No secret-fetching logic needed.
try:
    storage_client = storage.Client(project=PROJECT_ID)
    gemini_client = GeminiClient()
except Exception as e:
    logging.critical(f"A critical error occurred during client initialization: {e}")
    storage_client = gemini_client = None

def create_summaries(request):
    """HTTP-triggered Google Cloud Function entry point."""
    if not all([storage_client, gemini_client]):
        return "Server configuration error: clients not initialized.", 500

    run_pipeline(gemini_client=gemini_client, storage_client=storage_client)
    return "Transcript summarization pipeline started.", 202