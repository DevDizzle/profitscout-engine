import functions_framework
import pandas as pd
import json
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from google.cloud import bigquery, storage
from google.cloud import pubsub_v1
from google.api_core import retry
import datetime

# ─── Configuration ──────────────────────────────────────────────────────────────
SOURCE_PROJECT_ID       = os.environ.get("PROJECT_ID")              # Function + topic
DESTINATION_PROJECT_ID  = os.environ.get("DESTINATION_PROJECT_ID")  # BigQuery target
BUNDLE_BUCKET_NAME      = os.environ.get("BUNDLE_BUCKET_NAME")
BQ_METADATA_TABLE       = os.environ.get("BQ_METADATA_TABLE")
BQ_INDUSTRY_TABLE       = os.environ.get("BQ_INDUSTRY_TABLE")
BQ_SECTOR_TABLE         = os.environ.get("BQ_SECTOR_TABLE")
AGGREGATION_COMPLETE_TOPIC = os.environ.get("AGGREGATION_COMPLETE_TOPIC")
MAX_WORKERS             = 4

# ─── Initialize Clients ────────────────────────────────────────────────────────
bq_client      = bigquery.Client(project=DESTINATION_PROJECT_ID)
storage_client = storage.Client()
publisher      = pubsub_v1.PublisherClient()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# ─── Helpers ───────────────────────────────────────────────────────────────────
def get_bundle_list_from_bq():
    """Fetch list of bundles that have necessary metadata."""
    logging.info("Fetching bundle list from BigQuery …")
    query = f"""
        SELECT ticker, industry, sector, bundle_gcs_path
        FROM `{DESTINATION_PROJECT_ID}.{BQ_METADATA_TABLE}`
        WHERE bundle_gcs_path IS NOT NULL
          AND industry IS NOT NULL
          AND sector IS NOT NULL
    """
    try:
        df = bq_client.query(query).to_dataframe()
        logging.info("Found %d bundles to process.", len(df))
        return df.to_dict("records")
    except Exception as exc:
        logging.error("Failed to fetch bundle list: %s", exc)
        return []

def download_and_clean_bundle(bundle_info):
    """Download a JSON bundle from GCS and keep only numerical sections."""
    gcs_path = bundle_info.get("bundle_gcs_path")
    if not gcs_path:
        return None

    try:
        bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
        content = (
            storage_client.bucket(bucket_name)
            .blob(blob_name)
            .download_as_string()
        )
        data = json.loads(content)

        numerical_data = {
            "financials":  data.get("financial_statements", {}),
            "ratios":      data.get("ratios", {}),
            "metrics":     data.get("key_metrics", {}),
            "technicals":  data.get("technicals", {}),
            "prices":      data.get("prices", {}),
            "ticker":      bundle_info["ticker"],
            "industry":    bundle_info["industry"],
            "sector":      bundle_info["sector"],
        }
        return numerical_data

    except Exception as exc:
        logging.error("Failed to process %s: %s", gcs_path, exc)
        return None

def upload_json_to_gcs(bucket_name, blob_name, data):
    """Upload JSON data to GCS and return the URI."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    return f"gs://{bucket_name}/{blob_name}"

def aggregate_and_save_to_bq(df: pd.DataFrame, group_by_col: str, table_id: str):
    """Aggregate by column, save JSON to GCS, and load URI into BigQuery."""
    logging.info("Aggregating by %s …", group_by_col)
    results = []
    for name, group in df.groupby(group_by_col):
        ticker_count = len(group)
        ticker_data = group.to_dict(orient="records")
        # Upload to GCS
        blob_name = f"aggregated/{group_by_col}/{name.replace(' ', '_').lower()}/{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        ticker_data_uri = upload_json_to_gcs(BUNDLE_BUCKET_NAME, blob_name, ticker_data)
        results.append(
            {
                group_by_col: name,
                "ticker_count": ticker_count,
                "ticker_data": ticker_data_uri,  # URI to JSON
                "last_updated": datetime.datetime.utcnow(),
            }
        )

    if not results:
        logging.warning("No data produced for %s.", group_by_col)
        return

    aggregated_df = pd.DataFrame(results)

    schema = [
        bigquery.SchemaField(group_by_col, "STRING"),
        bigquery.SchemaField("ticker_count", "INTEGER"),
        bigquery.SchemaField("ticker_data", "STRING"),  # URI as STRING
        bigquery.SchemaField("last_updated", "TIMESTAMP"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )

    full_table_id = f"{DESTINATION_PROJECT_ID}.{table_id}"
    load_job = bq_client.load_table_from_dataframe(
        aggregated_df, full_table_id, job_config=job_config
    )
    load_job.result()
    logging.info("Loaded %d rows into %s.", len(aggregated_df), full_table_id)

def publish_completion_message():
    """Notify downstream services that aggregation finished."""
    try:
        topic_path = publisher.topic_path(SOURCE_PROJECT_ID,
                                          AGGREGATION_COMPLETE_TOPIC)
        future = publisher.publish(topic_path,
                                   b"Aggregation complete, ready to sync.")
        future.result()
        logging.info("Completion message published to %s.",
                     AGGREGATION_COMPLETE_TOPIC)
    except Exception as exc:
        logging.error("Failed to publish completion message: %s", exc)

# ─── Cloud Function Entry Point ────────────────────────────────────────────────
@functions_framework.http
def run(request):
    """HTTP‑triggered entry point for aggregation."""
    bundle_list = get_bundle_list_from_bq()
    if not bundle_list:
        return ("No bundles found to process.", 404)

    cleaned = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_and_clean_bundle, b)
                   for b in bundle_list]
        for fut in futures:
            result = fut.result()
            if result:
                cleaned.append(result)

    if not cleaned:
        return ("Failed to process any bundles.", 500)

    df = pd.json_normalize(cleaned)

    # Aggregate + load
    aggregate_and_save_to_bq(df, "industry", BQ_INDUSTRY_TABLE)
    aggregate_and_save_to_bq(df, "sector",   BQ_SECTOR_TABLE)

    # Publish message
    publish_completion_message()

    return ("Aggregation and BigQuery load complete.", 200)