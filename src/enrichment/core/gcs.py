# enrichment/core/gcs.py
"""
Shared helper functions for reading and writing blobs in GCS for all Enrichment services.
"""
from google.cloud import storage
import logging

def _client() -> storage.Client:
    """Initializes and returns a GCS client."""
    return storage.Client()

def blob_exists(bucket_name: str, blob_name: str, client: storage.Client | None = None) -> bool:
    """Checks if a blob exists in GCS."""
    try:
        # Use provided client or create a new one
        storage_client = client or _client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        logging.error(f"Failed to check existence for blob {blob_name}: {e}")
        return False

def read_blob(bucket_name: str, blob_name: str, encoding: str = "utf-8", client: storage.Client | None = None) -> str | None:
    """Reads a blob from GCS and returns its content as a string."""
    try:
        storage_client = client or _client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_text(encoding=encoding)
    except Exception as e:
        logging.error(f"Failed to read blob {blob_name}: {e}")
        return None

def write_text(bucket_name: str, blob_name: str, data: str, content_type: str = "text/plain", client: storage.Client | None = None) -> None:
    """Writes a string to a blob in GCS."""
    try:
        storage_client = client or _client()
        storage_client.bucket(bucket_name).blob(blob_name).upload_from_string(data, content_type=content_type)
    except Exception as e:
        logging.error(f"Failed to write blob {blob_name}: {e}")

def list_blobs(bucket_name: str, prefix: str | None = None, client: storage.Client | None = None) -> list[str]:
    """Lists all the blob names in a GCS bucket with a given prefix."""
    storage_client = client or _client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs]


def list_blobs_with_properties(bucket_name: str, prefix: str | None = None, client: storage.Client | None = None) -> dict[str, object]:
    """
    Lists blobs with their metadata properties (specifically 'updated' timestamp).
    Returns a dict: {blob_name: blob_updated_datetime}
    """
    storage_client = client or _client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    return {blob.name: blob.updated for blob in blobs}


def cleanup_old_files(bucket_name: str, folder: str, ticker: str, keep_filename: str):
    """Deletes all files for a ticker in a folder except for the one to keep."""
    # This function is less intensive usually, but we could update it too.
    # Leaving as-is for now to minimize diff, or update if passed client.
    client = _client()
    bucket = client.bucket(bucket_name)
    prefix = f"{folder}{ticker}_"
    
    blobs_to_delete = [
        blob for blob in bucket.list_blobs(prefix=prefix)
        if blob.name != keep_filename
    ]
    
    for blob in blobs_to_delete:
        logging.info(f"[{ticker}] Deleting old file: {blob.name}")
        blob.delete()
        
def list_blobs_with_content(bucket_name: str, prefix: str) -> dict:
    client = _client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    content_map = {}
    for blob in blobs:
        try:
            content = blob.download_as_text()
            content_map[blob.name] = content
        except Exception as e:
            logging.error(f"Failed to read blob {blob.name}: {e}")
    return content_map

def delete_all_in_prefix(bucket_name: str, prefix: str, client: storage.Client | None = None) -> None:
    """
    Deletes all blobs within a given prefix (folder) in a GCS bucket.
    Handles deletions in batches to avoid 'Too many deferred requests' errors.
    """
    try:
        logging.info(f"Starting cleanup for prefix: gs://{bucket_name}/{prefix}")
        storage_client = client or _client()
        bucket = storage_client.bucket(bucket_name)
        blobs_to_delete = list(bucket.list_blobs(prefix=prefix))

        if not blobs_to_delete:
            logging.info("Prefix is already empty. No files to delete.")
            return

        total_blobs = len(blobs_to_delete)
        batch_size = 100 # Safe limit well below 1000
        
        logging.info(f"Found {total_blobs} blobs to delete. Processing in batches of {batch_size}...")

        # Process in chunks
        for i in range(0, total_blobs, batch_size):
            batch_blobs = blobs_to_delete[i : i + batch_size]
            try:
                with storage_client.batch():
                    for blob in batch_blobs:
                        if blob.name != prefix:
                            blob.delete()
                logging.info(f"Deleted batch {i // batch_size + 1}: {len(batch_blobs)} blobs.")
            except Exception as e:
                logging.error(f"Batch deletion failed for batch starting at index {i}: {e}")
                # Continue to next batch instead of hard crash
                continue
        
        logging.info(f"Finished cleanup for prefix '{prefix}'.")
    except Exception as e:
        logging.error(f"Failed to list or delete blobs in prefix '{prefix}': {e}", exc_info=True)
        raise