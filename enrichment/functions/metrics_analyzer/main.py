import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import functions_framework
from .core import config, gcs, orchestrator, utils

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_blob(blob_name: str):
    """
    Defines the analysis workflow for a single key metrics blob.
    """
    try:
        logging.info(f"Processing: {blob_name}")
        ticker, date = utils.parse_filename(blob_name)
        
        # Read key metrics data from the source blob
        raw_content = gcs.read_blob(config.GCS_BUCKET, blob_name)
        key_metrics_data = utils.read_key_metrics_data(raw_content)

        if not key_metrics_data:
            logging.error(f"Failed to read or validate data from {blob_name}.")
            return None

        # Generate the analysis using the orchestrator
        analysis_json = orchestrator.summarise(key_metrics_data)
        
        # Write the analysis to the output location
        summary_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date}.json"
        gcs.write_text(config.GCS_BUCKET, summary_blob_path, analysis_json)
        
        return f"Successfully processed {blob_name} -> {summary_blob_path}"

    except Exception as e:
        logging.error(f"An unexpected error occurred processing {blob_name}: {e}", exc_info=True)
        return None

@functions_framework.http
def metrics_analyzer_function(request):
    """
    HTTP-triggered Cloud Function entry point.
    """
    logging.info("Starting key metrics analysis batch job...")
    
    all_input_blobs = gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_INPUT_PREFIX)
    all_output_blobs = set(gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_OUTPUT_PREFIX))

    work_items = []
    for blob_name in all_input_blobs:
        ticker, date = utils.parse_filename(blob_name)
        if not ticker or not date:
            continue
            
        summary_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date}.json"
        if summary_blob_path not in all_output_blobs:
            work_items.append(blob_name)

    if not work_items:
        logging.info("All key metrics analyses are already up-to-date.")
        return "All key metrics analyses are already up-to-date.", 200

    logging.info(f"Found {len(work_items)} new key metrics files to process.")

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, name) for name in work_items]
        
        successful_count = 0
        for future in as_completed(futures):
            result = future.result()
            if result:
                successful_count += 1
                logging.info(result)

    logging.info(f"Key metrics analysis batch job finished. Processed {successful_count} new files.")
    return f"Key metrics analysis process completed. Processed {successful_count} files.", 200