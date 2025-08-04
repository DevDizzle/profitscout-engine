import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import functions_framework
from .core import config, gcs, orchestrator, utils

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_blob(blob_name):
    """
    Defines the complete analysis workflow for a single GCS blob.
    """
    try:
        # 1. Skip if summary already exists
        ticker, date = utils.parse_filename(blob_name)
        if not ticker or not date:
            logging.warning(f"Skipping malformed filename: {blob_name}")
            return None

        summary_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date}.json"
        if gcs.blob_exists(config.GCS_BUCKET, summary_blob_path):
            logging.info(f"Summary already exists for {blob_name}, skipping.")
            return f"Skipped: {blob_name}"

        # 2. Read financial data from the source blob
        logging.info(f"Processing: {blob_name}")
        raw_content = gcs.read_blob(config.GCS_BUCKET, blob_name)
        financial_data = utils.read_financial_data(raw_content)

        if not financial_data:
            logging.error(f"Failed to read or validate data from {blob_name}.")
            return None

        # 3. Generate the analysis using the orchestrator
        analysis_json = orchestrator.summarise(financial_data)
        
        # 4. Write the analysis to the output location
        gcs.write_text(config.GCS_BUCKET, summary_blob_path, analysis_json)
        
        return f"Successfully processed {blob_name} -> {summary_blob_path}"

    except Exception as e:
        logging.error(f"An unexpected error occurred processing {blob_name}: {e}", exc_info=True)
        return None

def main():
    """
    Main function to run the financial analysis process.
    """
    logging.info("Starting financial analysis batch job...")
    
    blob_names = gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_INPUT_PREFIX)
    
    if not blob_names:
        logging.warning("No financial statements found to process.")
        return

    logging.info(f"Found {len(blob_names)} files to process in gs://{config.GCS_BUCKET}/{config.GCS_INPUT_PREFIX}")

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, name) for name in blob_names]
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                logging.info(result)

    logging.info("Financial analysis batch job finished.")

@functions_framework.http
def financials_analyzer_function(request):
    """
    HTTP-triggered Cloud Function entry point.
    """
    # The HTTP request body is not used, but the function signature is required.
    # This function will process all new financial statements when triggered.
    main()
    return "Financial analysis process started successfully.", 200