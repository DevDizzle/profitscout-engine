import base64
import json
import logging
from .core import config, gcs, orchestrator, utils

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def analyze_financial_statement(event, context):
    """
    Cloud Function triggered by a Pub/Sub message to analyze financial statements.
    It handles two possible message formats for maximum compatibility.
    """
    logging.info(f"Financials analyzer triggered by messageId: {context.event_id}")
    
    bucket_name = None
    blob_name = None

    # 1. Decode the Pub/Sub message
    try:
        message_data_str = base64.b64decode(event['data']).decode('utf-8')
        message_data = json.loads(message_data_str)

        # --- SMART PAYLOAD HANDLING ---
        # Check for the format from a live collector
        if "gcs_path" in message_data:
            gcs_path = message_data["gcs_path"]
            if gcs_path.startswith("gs://"):
                bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
            else:
                logging.error(f"Invalid GCS path in message: {message_data}")
                return "Invalid gcs_path format.", 400

        # Check for the format from a backfill script
        elif "bucket" in message_data and "name" in message_data:
            bucket_name = message_data["bucket"]
            blob_name = message_data["name"]

        else:
            logging.error(f"Unrecognized message format: {message_data}")
            return "Unrecognized message format.", 400

    except Exception as e:
        logging.error(f"Failed to decode or parse Pub/Sub message: {e}", exc_info=True)
        return "Failed to decode or parse message.", 400
    
    logging.info(f"Processing gs://{bucket_name}/{blob_name}")

    # 2. Check if analysis already exists
    # Assumes utils.parse_filename is adapted for 'TICKER_YYYY-MM-DD.json'
    ticker, date = utils.parse_filename(blob_name)
    if not ticker or not date:
        logging.warning(f"Skipping malformed filename: {blob_name}")
        return "Skipped due to malformed filename.", 200

    analysis_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date}.txt"
    if gcs.blob_exists(bucket_name, analysis_blob_path):
        logging.info(f"Analysis already exists for {blob_name}, skipping.")
        return "Analysis already exists.", 200

    # 3. Process the financial statement
    try:
        raw_json = gcs.read_blob(bucket_name, blob_name)
        # Assumes utils.read_financial_data simply returns the raw JSON content
        content = utils.read_financial_data(raw_json)

        if not content:
            logging.error(f"Failed to extract required data from {blob_name}.")
            return "Error parsing financial data.", 200

        # Assumes orchestrator.analyze takes content, ticker, and date
        analysis_text = orchestrator.analyze(content, ticker=ticker, date=date)
        gcs.write_text(config.GCS_BUCKET, analysis_blob_path, analysis_text)
        
        final_message = f"Successfully created analysis: {analysis_blob_path}"
        logging.info(final_message)
        return final_message, 200

    except Exception as e:
        logging.error(f"An unexpected error occurred processing {blob_name}: {e}", exc_info=True)
        raise e