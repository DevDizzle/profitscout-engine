# transcript_summarizer/main.py
"""
Pub/Sub-triggered Cloud Function that receives a message containing a GCS path
to a new transcript and generates a summary for it.
"""
import logging
import json
import base64
from .core import config, gcs, utils, orchestrator

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def create_transcript_summaries(event, context):
    """
    Cloud Function entry point triggered by a Pub/Sub message.
    The message is expected to contain the GCS path of the new transcript.
    """
    logging.info(f"Transcript summarizer triggered by messageId: {context.event_id}")

    # 1. Decode the Pub/Sub message
    try:
        # The message data is base64-encoded
        message_data_str = base64.b64decode(event['data']).decode('utf-8')
        message_data = json.loads(message_data_str)
        gcs_path = message_data.get("gcs_path")

        if not gcs_path or not gcs_path.startswith("gs://"):
            logging.error(f"Invalid GCS path in message: {message_data}")
            return "Invalid message format: missing or invalid gcs_path.", 400
            
    except Exception as e:
        logging.error(f"Failed to decode Pub/Sub message: {e}", exc_info=True)
        return "Failed to decode message.", 400
    
    # Extract bucket and blob name from the GCS path
    try:
        bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
    except ValueError:
        logging.error(f"Could not parse bucket and blob from GCS path: {gcs_path}")
        return "Invalid GCS path format.", 400

    logging.info(f"Processing transcript: {gcs_path}")

    # 2. Check if summary already exists (as a safeguard)
    ticker, date = utils.parse_filename(blob_name)
    if not ticker or not date:
        logging.warning(f"Skipping malformed filename: {blob_name}")
        return "Skipped due to malformed filename.", 200

    summary_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date}.txt"
    if gcs.blob_exists(config.GCS_BUCKET, summary_blob_path):
        logging.info(f"Summary already exists for {blob_name}, skipping.")
        return "Summary already exists.", 200

    # 3. Process the transcript
    try:
        # Read and parse the transcript JSON from the path received in the message
        raw_json = gcs.read_blob(bucket_name, blob_name)
        content, year, quarter = utils.read_transcript_data(raw_json)

        if not content or not year or not quarter:
            logging.error(f"Failed to extract required data from {blob_name}.")
            # Acknowledge the message so it's not retried
            return "Error parsing transcript data.", 200

        # Generate summary using the GenAI client
        summary_text = orchestrator.summarise(content, ticker=ticker, year=year, quarter=quarter)

        # Upload the new summary to GCS
        gcs.write_text(config.GCS_BUCKET, summary_blob_path, summary_text)
        
        final_message = f"Successfully created summary: {summary_blob_path}"
        logging.info(final_message)
        return final_message, 200

    except Exception as e:
        logging.error(f"An unexpected error occurred processing {blob_name}: {e}", exc_info=True)
        # Re-raise the exception to signal failure to Pub/Sub for potential retry
        raise e