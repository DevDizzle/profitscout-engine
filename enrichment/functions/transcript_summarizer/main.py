import logging
from .core import config, gcs, orchestrator, utils

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def create_missing_summaries(request):
    """
    Scans the GCS bucket for all transcripts and creates a summary for
    each one if a summary doesn't already exist.
    """
    logging.info("Starting batch transcript summarization process.")
    
    # 1. Get lists of all transcripts and all existing summaries
    #    This now correctly uses GCS_INPUT_PREFIX
    all_transcripts = set(gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_INPUT_PREFIX))
    all_summaries = set(gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_OUTPUT_PREFIX))
    logging.info(f"Found {len(all_transcripts)} transcripts and {len(all_summaries)} existing summaries.")

    summaries_created = 0
    # 2. Loop through each transcript
    for blob_name in all_transcripts:
        if not blob_name.endswith(".json"):
            continue

        try:
            # 3. Determine the expected summary path and check if it exists
            ticker, date = utils.parse_filename(blob_name)
            if not ticker or not date:
                logging.warning(f"Skipping malformed filename: {blob_name}")
                continue
            
            # This correctly uses GCS_OUTPUT_PREFIX
            summary_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date}.txt"
            
            if summary_blob_path in all_summaries:
                continue

            # 4. If summary doesn't exist, create it
            logging.info(f"Generating summary for {blob_name}...")
            raw_json = gcs.read_blob(config.GCS_BUCKET, blob_name)
            content, year, quarter = utils.read_transcript_data(raw_json)

            if not content or not year or not quarter:
                logging.error(f"Failed to extract required data from {blob_name}.")
                continue

            summary_text = orchestrator.summarise(content, ticker=ticker, year=year, quarter=quarter)
            gcs.write_text(config.GCS_BUCKET, summary_blob_path, summary_text)
            summaries_created += 1

        except Exception as e:
            logging.error(f"An error occurred processing {blob_name}: {e}", exc_info=True)
            continue

    final_message = f"Batch process complete. Created {summaries_created} new summaries."
    logging.info(final_message)
    return final_message, 200