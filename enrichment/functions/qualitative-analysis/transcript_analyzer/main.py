import logging
import functions_framework
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from .core import config, gcs

def _get_latest_summaries_to_process(bq_client: bigquery.Client) -> list[dict]:
    """
    Queries BigQuery to get the latest quarter_end_date for each ticker
    and constructs the expected GCS path for the summary file.
    """
    logging.info(f"Querying BigQuery for latest metadata: {config.BQ_METADATA_TABLE}")
    query = f"""
        SELECT ticker, MAX(quarter_end_date) as max_date
        FROM `{config.BQ_METADATA_TABLE}`
        GROUP BY ticker
    """
    try:
        results = bq_client.query(query).to_dataframe()
        work_list = [
            {
                "ticker": row["ticker"],
                "date_str": row["max_date"].strftime('%Y-%m-%d')
            }
            for _, row in results.iterrows()
        ]
        logging.info(f"Found {len(work_list)} tickers with latest dates from BigQuery.")
        return work_list
    except Exception as e:
        logging.error(f"Failed to fetch latest ticker data from BigQuery: {e}")
        return []

def process_summary_blob(work_item: dict):
    """Worker function to process a single transcript summary."""
    try:
        ticker = work_item["ticker"]
        date_str = work_item["date_str"]

        summary_blob_path = f"{config.GCS_INPUT_PREFIX}{ticker}_{date_str}.txt"
        analysis_blob_path = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date_str}.json"

        # Check if the analysis already exists
        if gcs.blob_exists(config.GCS_BUCKET, analysis_blob_path):
            return None # Already processed

        # Check if the source summary exists before proceeding
        if not gcs.blob_exists(config.GCS_BUCKET, summary_blob_path):
            logging.warning(f"Source summary not found, skipping: {summary_blob_path}")
            return None

        logging.info(f"Generating transcript analysis for {summary_blob_path}...")
        summary_content = gcs.read_blob(config.GCS_BUCKET, summary_blob_path)

        if not summary_content.strip():
            logging.warning(f"Summary file is empty: {summary_blob_path}. Creating neutral analysis.")
            summary_content = "No content available."
        
        # Late import to avoid circular dependency issues in some environments
        from .core import orchestrator
        analysis_json = orchestrator.summarise(summary_content)
        gcs.write_text(config.GCS_BUCKET, analysis_blob_path, analysis_json)
        
        return analysis_blob_path
    except Exception as e:
        logging.error(f"An error occurred processing {work_item}: {e}", exc_info=True)
        return None

@functions_framework.http
def transcript_analyzer(request):
    """
    HTTP-triggered function to find and create analyses for the MOST RECENT
    earnings call summaries based on BigQuery metadata.
    """
    logging.info("Starting batch transcript analysis process.")
    
    bq_client = bigquery.Client(project=config.PROJECT_ID)
    work_items = _get_latest_summaries_to_process(bq_client)
    
    if not work_items:
        return "No work items found from BigQuery. Exiting.", 200

    analyses_created = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = {executor.submit(process_summary_blob, item): item for item in work_items}
        for future in as_completed(futures):
            if future.result():
                analyses_created += 1

    final_message = f"Batch process complete. Created {analyses_created} new transcript analyses."
    logging.info(final_message)
    return final_message, 200