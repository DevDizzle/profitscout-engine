# enrichment/functions/transcript_summarizer/main.py

import logging
import concurrent.futures
from .core import config, gcs, orchestrator, utils

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_transcript(blob_name, all_summaries):
    """
    Worker function to process a single transcript.
    This function is executed by each thread in the pool.
    """
    try:
        # Determine the expected summary path and check if it already exists
        ticker, date = utils.parse_filename(blob_name)
        if not ticker or not date:
            logging.warning(f"Skipping malformed filename: {blob_name}")
            return None # Return None for skipped files

        summary_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date}.txt"
        
        # If the summary is in the set of existing summaries, we skip it
        if summary_blob_path in all_summaries:
            return None # Return None for skipped files

        # If summary doesn't exist, create it
        logging.info(f"Generating summary for {blob_name}...")
        raw_json = gcs.read_blob(config.GCS_BUCKET, blob_name)
        content, year, quarter = utils.read_transcript_data(raw_json)

        if not content or not year or not quarter:
            logging.error(f"Failed to extract required data from {blob_name}.")
            return None

        summary_text = orchestrator.summarise(content, ticker=ticker, year=year, quarter=quarter)
        gcs.write_text(config.GCS_BUCKET, summary_blob_path, summary_text)
        
        return summary_blob_path # Return the path of the created summary

    except Exception as e:
        logging.error(f"An error occurred processing {blob_name}: {e}", exc_info=True)
        return None


def create_missing_summaries(request):
    """
    Scans GCS for transcripts and uses a thread pool to create missing
    summaries in parallel.
    """
    logging.info("Starting batch transcript summarization process.")
    
    # 1. Get lists of all transcripts and existing summaries
    all_transcripts = gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_INPUT_PREFIX)
    all_summaries = set(gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_OUTPUT_PREFIX))
    logging.info(f"Found {len(all_transcripts)} transcripts and {len(all_summaries)} existing summaries.")

    # Filter out non-json files
    json_transcripts = [blob for blob in all_transcripts if blob.endswith(".json")]

    summaries_created = 0
    # 2. Use a ThreadPoolExecutor to process files in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        # Create a future for each transcript processing task
        future_to_blob = {executor.submit(process_transcript, blob_name, all_summaries): blob_name for blob_name in json_transcripts}

        for future in concurrent.futures.as_completed(future_to_blob):
            result = future.result()
            if result: # If a summary was created, the result is not None
                summaries_created += 1

    final_message = f"Batch process complete. Created {summaries_created} new summaries."
    logging.info(final_message)
    return final_message, 200