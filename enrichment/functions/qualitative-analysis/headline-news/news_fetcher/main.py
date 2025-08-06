import base64
import json
import logging
from .core import config, fetcher
import functions_framework

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Global Initialization ---
FMP_API_KEY = None
try:
    with open(f"/secrets/{config.FMP_API_KEY_SECRET}", "r") as f:
        FMP_API_KEY = f.read().strip()
except (FileNotFoundError, IOError) as e:
    logging.critical(f"Could not read FMP API Key from Secret Manager: {e}")

@functions_framework.cloud_event
def news_fetcher_function(cloud_event):
    """
    Cloud Function triggered by a Pub/Sub message to fetch and save news headlines.
    """
    if not FMP_API_KEY:
        logging.critical("FMP_API_KEY is not available. Aborting.")
        return "Server configuration error.", 500

    try:
        message_data_str = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
        message_data = json.loads(message_data_str)
        ticker = message_data.get("ticker")
        query = message_data.get("query")

        if not all([ticker, query]):
            logging.error("Message is missing 'ticker' or 'query'.")
            return "Invalid message format.", 400

        logging.info(f"Fetching news for ticker: {ticker}")

        gcs_uri = fetcher.fetch_and_save_headlines(
            ticker=ticker,
            query_str=query,
            api_key=FMP_API_KEY,
            bucket_name=config.GCS_BUCKET,
            output_prefix=config.GCS_OUTPUT_PREFIX
        )

        logging.info(f"Successfully fetched and saved headlines to: {gcs_uri}")
        return "Headlines processed successfully.", 200

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        raise