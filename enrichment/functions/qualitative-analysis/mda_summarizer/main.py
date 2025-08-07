import logging
import functions_framework
from concurrent.futures import ThreadPoolExecutor, as_completed
from .core import config, gcs, orchestrator, utils

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_mda_blob(blob_name: str, existing_summaries: set):
    """Worker function to process a single MD&A blob."""
    try:
        ticker, date_str, filing_type, year, quarter = utils.parse_filename(blob_name)
        if not all([ticker, date_str, filing_type, year, quarter]):
            logging.warning(f"Skipping malformed filename: {blob_name}")
            return None

        # Use a consistent output format
        summary_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date_str}_{filing_type}.txt"
        if summary_blob_path in existing_summaries:
            return None # Already processed

        logging.info(f"Generating MD&A summary for {blob_name}...")
        raw_json = gcs.read_blob(config.GCS_BUCKET, blob_name)
        mda_content = utils.read_mda_data(raw_json)

        if not mda_content:
            logging.error(f"Failed to extract MD&A content from {blob_name}.")
            return None

        summary_text = orchestrator.summarise(mda_content, ticker, year, quarter, filing_type)
        gcs.write_text(config.GCS_BUCKET, summary_blob_path, summary_text)
        
        return summary_blob_path
    except Exception as e:
        logging.error(f"An error occurred processing {blob_name}: {e}", exc_info=True)
        return None

@functions_framework.http
def mda_summarizer(request):
    """
    HTTP-triggered function to find and create missing MD&A summaries.
    """
    logging.info("Starting batch MD&A summarization process.")
    
    all_mda_blobs = gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_INPUT_PREFIX)
    all_summaries = set(gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_OUTPUT_PREFIX))
    logging.info(f"Found {len(all_mda_blobs)} MD&A files and {len(all_summaries)} existing summaries.")

    summaries_created = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        future_to_blob = {
            executor.submit(process_mda_blob, blob_name, all_summaries): blob_name
            for blob_name in all_mda_blobs if blob_name.endswith(".json")
        }
        for future in as_completed(future_to_blob):
            if future.result():
                summaries_created += 1

    final_message = f"Batch process complete. Created {summaries_created} new MD&A summaries."
    logging.info(final_message)
    return final_message, 200