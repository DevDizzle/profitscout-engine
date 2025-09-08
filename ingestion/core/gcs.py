# enrichment/core/gcs.py
"""
Shared helper functions for reading and writing blobs in GCS for all Enrichment services.
"""

from typing import Dict, List, Optional
import logging
from google.cloud import storage

logger = logging.getLogger(__name__)


def _client() -> storage.Client:
    """Initializes and returns a GCS client."""
    return storage.Client()


def blob_exists(bucket_name: str, blob_name: str) -> bool:
    """Checks if a blob exists in GCS."""
    try:
        bucket = _client().bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        logger.error(f"Failed to check existence for blob {blob_name}: {e}")
        return False


def read_blob(bucket_name: str, blob_name: str, encoding: str = "utf-8") -> Optional[str]:
    """Reads a blob from GCS and returns its content as a string."""
    try:
        bucket = _client().bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_text(encoding=encoding)
    except Exception as e:
        logger.error(f"Failed to read blob {blob_name}: {e}")
        return None


def write_text(bucket_name: str, blob_name: str, data: str, content_type: str = "text/plain") -> None:
    """Writes a string to a blob in GCS."""
    try:
        _client().bucket(bucket_name).blob(blob_name).upload_from_string(
            data, content_type=content_type
        )
    except Exception as e:
        logger.error(f"Failed to write blob {blob_name}: {e}")
        raise


def list_blobs(bucket_name: str, prefix: Optional[str] = None) -> List[str]:
    """Lists all the blob names in a GCS bucket with a given prefix."""
    blobs = _client().list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs]


def cleanup_old_files(bucket_name: str, folder: str, ticker: str, keep_filename: str) -> None:
    """Deletes all files for a ticker in a folder except for the one to keep."""
    client = _client()
    bucket = client.bucket(bucket_name)
    prefix = f"{folder}{ticker}_"

    blobs_to_delete = [
        blob for blob in bucket.list_blobs(prefix=prefix)
        if blob.name != keep_filename
    ]

    for blob in blobs_to_delete:
        logger.info(f"[{ticker}] Deleting old file: {blob.name}")
        try:
            blob.delete()
        except Exception as e:
            logger.error(f"Failed to delete blob {blob.name}: {e}")


def list_blobs_with_content(bucket_name: str, prefix: str, encoding: str = "utf-8") -> Dict[str, str]:
    """Returns a mapping of blob name -> text content for blobs under a prefix."""
    client = _client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    content_map: Dict[str, str] = {}

    for blob in blobs:
        try:
            content = blob.download_as_text(encoding=encoding)
            content_map[blob.name] = content
        except Exception as e:
            logger.error(f"Failed to read blob {blob.name}: {e}")

    return content_map
