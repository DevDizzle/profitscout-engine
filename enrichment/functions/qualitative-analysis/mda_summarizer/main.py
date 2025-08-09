import logging
import functions_framework
from concurrent.futures import ThreadPoolExecutor, as_completed
from .core import config, gcs, orchestrator, utils

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_mda_blob(blob_name: str):
    """Worker function to process a single MD&A blob."""
    try:
        ticker, date_str, filing_type, year, quarter = utils.parse_filename(blob_name)
        if not all([ticker, date_str, filing_type, year, quarter]):
            logging.warning(f"Skipping malformed filename: {blob_name}")
            return None

        summary_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date_str}.txt"

        logging.info(f"Generating MD&A summary for {blob_name}...")
        raw_json = gcs.read_blob(config.GCS_BUCKET, blob_name)
        mda_content = utils.read_mda_data(raw_json)

        if not mda_content:
            logging.error(f"Failed to extract MD&A content from {blob_name}.")
            return None

        summary_text = orchestrator.summarise(mda_content, ticker, year, quarter, filing_type)
        gcs.write_text(config.GCS_BUCKET, summary_blob_path, summary_text)
        
        # --- ADDED: Cleanup logic to remove old summaries ---
        gcs.cleanup_old_files(config.GCS_BUCKET, config.GCS_OUTPUT_PREFIX, ticker, summary_blob_path)
        
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
    
    work_items = []
    for blob_name in all_mda_blobs:
        if not blob_name.endswith(".json"):
            continue
        ticker, date_str, _, _, _ = utils.parse_filename(blob_name)
        if not ticker or not date_str:
            continue
        
        expected_summary_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date_str}.txt"
        if expected_summary_path not in all_summaries:
            work_items.append(blob_name)

    if not work_items:
        logging.info("All MD&A summaries are up-to-date.")
        return "All MD&A summaries are up-to-date.", 200

    logging.info(f"Found {len(work_items)} new MD&A files to process.")
    summaries_created = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        future_to_blob = {
            executor.submit(process_mda_blob, blob_name): blob_name for blob_name in work_items
        }
        for future in as_completed(future_to_blob):
            if future.result():
                summaries_created += 1

    final_message = f"Batch process complete. Created {summaries_created} new MD&A summaries."
    logging.info(final_message)
    return final_message, 200