# transcript_summarizer/main.py
"""
HTTP-triggered Cloud Function that processes transcripts from GCS,
generating summaries for any that are missing.
"""
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud.storage import Blob
from .core import config, gcs, utils, orchestrator

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def _process_blob(blob: Blob):
    """
    Worker function to process a single transcript blob. It checks for an
    existing summary and creates one if it's missing.
    """
    blob_name = blob.name
    ticker, date = utils.parse_filename(blob_name)
    if not ticker or not date:
        logging.warning(f"Skipping malformed filename: {blob_name}")
        return "skipped_malformed"

    # Check if summary already exists
    summary_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date}.txt"
    if gcs.blob_exists(config.GCS_BUCKET, summary_blob_path):
        logging.info(f"Summary already exists for {blob_name}, skipping.")
        return "skipped_exists"

    try:
        # Read and parse the transcript JSON
        logging.info(f"Processing transcript: {blob_name}")
        raw_json = gcs.read_blob(config.GCS_BUCKET, blob_name)
        content, year, quarter = utils.read_transcript_data(raw_json)

        if not content or not year or not quarter:
            logging.error(f"Failed to extract required data from {blob_name}.")
            return "error_parsing"

        # Generate summary
        summary_text = orchestrator.summarise(content, ticker=ticker, year=year, quarter=quarter)

        # Upload the new summary
        gcs.write_text(config.GCS_BUCKET, summary_blob_path, summary_text)
        logging.info(f"Successfully created summary: {summary_blob_path}")
        return "processed"

    except Exception as e:
        logging.error(f"An unexpected error occurred processing {blob_name}: {e}", exc_info=True)
        return "error_unexpected"

def create_transcript_summaries(request):
    """
    Cloud Function entry point. Lists all transcripts and creates summaries for
    any that are missing. The function is idempotent.
    """
    logging.info("Transcript summarizer function triggered.")

    # Get all transcript files from GCS
    blobs_iterator = gcs.list_blobs(config.GCS_BUCKET, config.GCS_INPUT_PREFIX)
    
    # Convert the iterator to a list to get its length and to iterate over it multiple times if needed.
    transcript_blobs = list(blobs_iterator)
    
    if not transcript_blobs:
        return ("No transcripts found to process.", 200)

    # Now it is safe to call len() on the list
    logging.info(f"Found {len(transcript_blobs)} transcripts to check.")
    
    # Process blobs in parallel
    results = {"processed": 0, "skipped_exists": 0, "skipped_malformed": 0, "error_parsing": 0, "error_unexpected": 0}
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        future_to_blob = {executor.submit(_process_blob, blob): blob for blob in transcript_blobs}
        for future in as_completed(future_to_blob):
            blob_name = future_to_blob[future].name
            try:
                result = future.result()
                if result in results:
                    results[result] += 1
            except Exception as e:
                logging.error(f"Future for blob {blob_name} failed: {e}", exc_info=True)
                results["error_unexpected"] += 1

    final_message = f"Processing complete. Results: {json.dumps(results)}"
    logging.info(final_message)
    return (final_message, 200)