#!/usr/bin/env python3
"""
Cloud Function to sync asset metadata from BigQuery to Firestore.

Triggered by a Pub/Sub message from the 'bundle-updated' topic, which
contains the ticker symbol of the newly processed asset.
"""
import base64
import json
from google.cloud import bigquery, firestore
import functions_framework
from cloudevents.http import CloudEvent

from config import BQ_DATASET, BQ_PROJECT_ID, BQ_TABLE, FIRESTORE_COLLECTION

@functions_framework.cloud_event
def run(cloud_event: CloudEvent) -> None:
    """
    Main entry point for the Cloud Function.
    """
    try:
        # 1. Decode the ticker from the Pub/Sub message
        message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        payload = json.loads(message_data)
        ticker = payload.get("ticker")

        if not ticker:
            print("[ERROR] No ticker found in Pub/Sub message.")
            return

        print(f"Firestore sync triggered for ticker: {ticker}")

        # 2. Fetch the metadata from BigQuery
        client = bigquery.Client(project=BQ_PROJECT_ID)
        table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
        
        query = f"SELECT * FROM `{table_id}` WHERE ticker = @ticker LIMIT 1"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
        )
        
        query_job = client.query(query, job_config=job_config)
        rows = list(query_job.result())

        if not rows:
            print(f"[ERROR] No metadata found in BigQuery for ticker: {ticker}")
            return
            
        # Convert the BigQuery Row to a dictionary, handling data types
        data_to_sync = dict(rows[0])
        for key, value in data_to_sync.items():
            if hasattr(value, 'isoformat'): # Handles DATE, DATETIME, TIMESTAMP
                data_to_sync[key] = value.isoformat()

        # 3. Write the data to Firestore
        fs = firestore.Client()
        doc_ref = fs.collection(FIRESTORE_COLLECTION).document(ticker.upper())
        doc_ref.set(data_to_sync)

        print(f"[SUCCESS] Successfully synced metadata for {ticker} to Firestore.")

    except Exception as e:
        print(f"[FATAL] An unexpected error occurred: {e}")