# serving/core/gcs.py
import logging
import json
from datetime import date
from google.cloud import storage
from google.cloud.storage import Blob
from . import config


def _client() -> storage.Client:
    return storage.Client()


def list_blobs(bucket_name: str, prefix: str | None = None) -> list[str]:
    """Lists all the blob names in a GCS bucket with a given prefix."""
    blobs = _client().list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs]


def read_blob(bucket_name: str, blob_name: str, encoding: str = "utf-8") -> str | None:
    """Reads a blob from GCS and returns its content as a string."""
    try:
        bucket = _client().bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_text(encoding=encoding)
    except Exception as e:
        logging.error(f"Failed to read blob {blob_name}: {e}")
        return None


def write_text(
    bucket_name: str, blob_name: str, data: str, content_type: str = "text/plain"
):
    """Writes a string to a GCS blob."""
    try:
        _client().bucket(bucket_name).blob(blob_name).upload_from_string(
            data, content_type
        )
    except Exception as e:
        logging.error(f"Failed to write to blob {blob_name}: {e}")
        raise


def delete_blob(bucket_name: str, blob_name: str):
    """Deletes a blob from the bucket."""
    try:
        bucket = _client().bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        logging.info(f"Blob {blob_name} deleted.")
    except Exception as e:
        logging.error(f"Failed to delete blob {blob_name}: {e}")
        raise


def get_tickers() -> list[str]:
    """Loads the official ticker list from the GCS bucket."""
    try:
        bucket = _client().bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(config.TICKER_LIST_PATH)
        content = blob.download_as_text(encoding="utf-8")
        return [line.strip().upper() for line in content.splitlines() if line.strip()]
    except Exception as e:
        logging.error(f"Failed to load tickers from GCS: {e}")
        return []


def upload_from_filename(
    bucket_name: str,
    source_file_path: str,
    destination_blob_name: str,
    content_type: str = "image/png",
) -> str | None:
    """Uploads a local file to GCS and returns its GCS URI."""
    try:
        client = _client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        # --- MODIFIED LINE ---
        blob.upload_from_filename(source_file_path, content_type=content_type)
        return f"gs://{bucket_name}/{destination_blob_name}"
    except Exception as e:
        logging.error(f"Failed to upload {source_file_path} to GCS: {e}", exc_info=True)
        return None


def get_latest_blob_for_ticker(
    bucket_name: str, prefix: str, ticker: str
) -> Blob | None:
    """Finds the most recent blob for a ticker in a given folder."""
    client = _client()
    blobs = client.list_blobs(bucket_name, prefix=f"{prefix}{ticker}_")

    latest_blob = None
    latest_date = None

    for blob in blobs:
        try:
            date_str = blob.name.split("_")[-1].split(".")[0]
            blob_date = date.fromisoformat(date_str)
            if latest_date is None or blob_date > latest_date:
                latest_date = blob_date
                latest_blob = blob
        except (ValueError, IndexError):
            continue

    return latest_blob
