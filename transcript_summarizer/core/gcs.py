# transcript_summarizer/core/gcs.py
import logging
import json
from google.cloud import storage

def list_input_blobs(storage_client: storage.Client, bucket_name: str, input_folder: str) -> list[storage.Blob]:
    """Lists all JSON blobs in the specified input folder."""
    bucket = storage_client.bucket(bucket_name)
    return list(bucket.list_blobs(prefix=input_folder))

def blob_exists(storage_client: storage.Client, bucket_name: str, blob_path: str) -> bool:
    """Checks if a blob exists in GCS."""
    bucket = storage_client.bucket(bucket_name)
    return storage.Blob(bucket=bucket, name=blob_path).exists(storage_client)

def download_transcript_text(blob: storage.Blob) -> str:
    """Downloads a GCS blob, parses it as JSON, and returns the transcript content."""
    try:
        data = json.loads(blob.download_as_text(encoding="utf-8"))
        # Check common keys for transcript content
        for key in ("content", "transcript", "text", "body"):
            if isinstance(data, dict) and key in data and data[key]:
                return data[key]
        # Fallback for unexpected JSON structures
        return json.dumps(data)
    except Exception as e:
        logging.error(f"Failed to download or parse blob {blob.name}: {e}")
        return ""

def upload_summary(storage_client: storage.Client, bucket_name: str, summary_text: str, blob_path: str):
    """Uploads the text summary to GCS."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(summary_text, content_type="text/plain")
    logging.info(f"Successfully uploaded summary to gs://{bucket_name}/{blob_path}")