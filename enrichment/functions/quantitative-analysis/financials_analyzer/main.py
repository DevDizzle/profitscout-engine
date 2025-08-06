import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import functions_framework
from .core import config, gcs, orchestrator, utils

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_blob(blob_name: str):
    """
    Defines the analysis workflow for a single blob that is already
    confirmed to be missing a summary.
    """
    try:
        logging.info(f"Processing: {blob_name}")
        ticker, date = utils.parse_filename(blob_name)
        
        # 2. Read financial data from the source blob
        raw_content = gcs.read_blob(config.GCS_BUCKET, blob_name)
        financial_data = utils.read_financial_data(raw_content)

        if not financial_data:
            logging.error(f"Failed to read or validate data from {blob_name}.")
            return None

        # 3. Generate the analysis using the orchestrator
        analysis_json = orchestrator.summarise(financial_data)
        
        # 4. Write the analysis to the output location
        summary_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date}.json"
        gcs.write_text(config.GCS_BUCKET, summary_blob_path, analysis_json)
        
        return f"Successfully processed {blob_name} -> {summary_blob_path}"

    except Exception as e:
        logging.error(f"An unexpected error occurred processing {blob_name}: {e}", exc_info=True)
        return None

@functions_framework.http
def financials_analyzer_function(request):
    """
    HTTP-triggered Cloud Function entry point.
    Determines the list of missing summaries and processes them in parallel.
    """
    logging.info("Starting financial analysis batch job...")
    
    # 1. Get all potential input files and all existing output files
    all_input_blobs = gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_INPUT_PREFIX)
    all_output_blobs = set(gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_OUTPUT_PREFIX))

    # 2. Determine the actual work to be done BEFORE starting the workers
    work_items = []
    for blob_name in all_input_blobs:
        ticker, date = utils.parse_filename(blob_name)
        if not ticker or not date:
            continue
            
        summary_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date}.json"
        if summary_blob_path not in all_output_blobs:
            work_items.append(blob_name)

    if not work_items:
        logging.info("All financial analyses are already up-to-date.")
        return "All financial analyses are already up-to-date.", 200

    logging.info(f"Found {len(work_items)} new financial statements to process.")

    # 3. Process the filtered list of work items in parallel
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, name) for name in work_items]
        
        successful_count = 0
        for future in as_completed(futures):
            result = future.result()
            if result:
                successful_count += 1
                logging.info(result)

    logging.info(f"Financial analysis batch job finished. Processed {successful_count} new files.")
    return f"Financial analysis process completed successfully. Processed {successful_count} files.", 200