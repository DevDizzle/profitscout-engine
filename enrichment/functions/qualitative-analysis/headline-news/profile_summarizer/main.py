import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import functions_framework
from google.cloud import pubsub_v1
from .core import config, gcs, orchestrator, utils

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize Publisher Client Globally
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(config.PROJECT_ID, config.OUTPUT_TOPIC_ID)


def process_blob(blob_name: str):
    """
    Generates a query from a business profile and publishes it for the news_fetcher.
    """
    try:
        logging.info(f"Processing: {blob_name}")
        ticker = utils.parse_filename(blob_name)
        if not ticker:
            return None

        raw_content = gcs.read_blob(config.GCS_BUCKET, blob_name)
        business_profile_data = utils.read_business_profile_data(raw_content)

        if not business_profile_data:
            logging.error(f"Failed to read or validate data from {blob_name}.")
            return None

        # Get the query string directly from the LLM.
        boolean_query = orchestrator.summarise(business_profile_data)

        # Basic check to ensure the LLM didn't return an empty string.
        if not boolean_query or not boolean_query.strip():
            logging.warning(f"LLM returned an empty query for {ticker}. Using ticker as fallback.")
            boolean_query = f'"{ticker}"'

        # Publish the message with the ticker and the generated query.
        message_payload = json.dumps({"ticker": ticker, "query": boolean_query}).encode("utf-8")
        future = publisher.publish(topic_path, message_payload)
        future.result()

        return f"Successfully processed and published query for {ticker}."

    except Exception as e:
        logging.error(f"An unexpected error occurred processing {blob_name}: {e}", exc_info=True)
        return None

@functions_framework.http
def profile_summarizer_function(request):
    """
    HTTP-triggered Cloud Function entry point.
    """
    logging.info("Starting profile summarization batch job...")
    all_input_blobs = gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_INPUT_PREFIX)
    all_output_blobs = set(gcs.list_blobs(config.GCS_BUCKET, prefix=config.GCS_OUTPUT_PREFIX))
    work_items = [
        blob_name for blob_name in all_input_blobs
        if f"{config.GCS_OUTPUT_PREFIX}{utils.parse_filename(blob_name)}_profile_summary.json" not in all_output_blobs
        and utils.parse_filename(blob_name) is not None
    ]

    if not work_items:
        logging.info("All profile summaries are already up-to-date.")
        return "All profile summaries are already up-to-date.", 200

    logging.info(f"Found {len(work_items)} new business profiles to process.")

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, name) for name in work_items]
        successful_count = sum(1 for future in as_completed(futures) if future.result())

    logging.info(f"Profile summarization batch job finished. Processed {successful_count} new files.")
    return f"Profile summarization process completed. Processed {successful_count} files.", 200