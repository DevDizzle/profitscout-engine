import base64
import json
import logging
import functions_framework
from .core import config, gcs, builder
from google.cloud import pubsub_v1

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize Publisher Client Globally
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(config.PROJECT_ID, config.OUTPUT_TOPIC_ID)

@functions_framework.cloud_event
def query_builder_function(cloud_event): # <-- FIX 1: Changed to a single argument
    """
    Cloud Function triggered by a Pub/Sub message to build a boolean query
    and publish it for the next service.
    """
    try:
        message_data_str = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
        message_data = json.loads(message_data_str)
        gcs_path = message_data.get("gcs_path")

        if not gcs_path:
            logging.error("Message is missing 'gcs_path'.")
            return "Invalid message format.", 400

        bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
        logging.info(f"Processing summary file: {gcs_path}")

        # 2. Read the profile summary from GCS
        profile_json_str = gcs.read_blob(bucket_name, blob_name)

        if not profile_json_str or not profile_json_str.strip():
            logging.warning(f"Skipping empty or whitespace-only file: {gcs_path}")
            return "Skipped empty file.", 200

        profile_data = json.loads(profile_json_str)

        # 3. Build the boolean query
        boolean_query = builder.build_fmp_boolean_query(profile_data)
        ticker = blob_name.split('/')[-1].split('_')[0]

        # 4. Save the query to the output location
        output_blob_name = f"{config.GCS_OUTPUT_PREFIX}{ticker}_query.txt"
        gcs.write_text(config.GCS_BUCKET, output_blob_name, boolean_query)
        logging.info(f"Successfully created query file at gs://{config.GCS_BUCKET}/{output_blob_name}")

        # 5. Publish a message with the ticker and query
        message_payload = json.dumps({"ticker": ticker, "query": boolean_query}).encode("utf-8")
        future = publisher.publish(topic_path, message_payload)
        future.result()
        logging.info(f"Published query for {ticker} to topic {config.OUTPUT_TOPIC_ID}.")

        return "Query created and published successfully.", 200

    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON from file {gcs_path}: {e}")
        return "JSON Decode Error.", 200
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        raise