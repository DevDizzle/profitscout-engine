import logging
import functions_framework
from concurrent.futures import ThreadPoolExecutor, as_completed
from .core import config, gcs, orchestrator, utils

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_mda_summary_blob(blob_name: str, existing_analyses: set):
    """Worker function to process a single MD&A summary blob."""
    try:
        ticker, date_str, filing_type = utils.parse_filename(blob_name)
        if not all([ticker, date_str, filing_type]):
            logging.warning(f"Skipping malformed filename: {blob_name}")
            return None

        analysis_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date_str}_{filing_type}.json"
        if analysis_blob_path in existing_analyses:
            return None # Already processed

        logging.info(f"Generating MD&A analysis for {blob_name}...")
        raw_content = gcs.read_blob(config.GCS_BUCKET, blob_name)
        mda_summary = utils.read_mda_summary_data(raw_content)

        if not mda_summary:
            logging.warning(f"MD&A summary file is empty: {blob_name}. Creating neutral analysis.")
            mda_summary = "No content available."
        
        analysis_json = orchestrator.summarise(mda_summary)
        gcs.write_text(config.GCS_BUCKET, analysis_blob_path, analysis_json)
        
        return analysis_blob_path
    except Exception as e:
        logging.error(f"An error occurred processing {blob_name}: {e}", exc_info=True)
        return None

@functions_framework.http
def mda_analyzer(request):
    """
    HTTP-triggered function to find and create missing MD&A analyses.
    """
    logging.info("Starting batch MD&A analysis process.")
    
    all_summary_blobs = gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_INPUT_PREFIX)
    all_analyses = set(gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_OUTPUT_PREFIX))
    logging.info(f"Found {len(all_summary_blobs)} MD&A summaries and {len(all_analyses)} existing analyses.")

    analyses_created = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        future_to_blob = {
            executor.submit(process_mda_summary_blob, blob_name, all_analyses): blob_name
            for blob_name in all_summary_blobs if blob_name.endswith(".txt")
        }
        for future in as_completed(future_to_blob):
            if future.result():
                analyses_created += 1

    final_message = f"Batch process complete. Created {analyses_created} new MD&A analyses."
    logging.info(final_message)
    return final_message, 200