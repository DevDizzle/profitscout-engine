# rename_recommendations.py
import logging
from google.cloud import storage
import re

# --- Configuration ---
BUCKET_NAME = "profit-scout-data"
PREFIX = "recommendations/"
DEFAULT_DATE = "2025-08-25"
# ---------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def rename_old_recommendations():
    """
    Finds recommendation files without a date in their name and renames them
    to a standardized dated format (e.g., TICKER_recommendation_YYYY-MM-DD.md).
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=PREFIX)

    # Regex to find filenames that do NOT end with a date pattern
    # e.g., matches 'AAL_recommendation.md' but not 'ALB_recommendation_2025-08-26.md'
    old_format_regex = re.compile(r'([A-Z]+)_recommendation\.md$')

    logging.info(f"Scanning for files in gs://{BUCKET_NAME}/{PREFIX}...")
    count = 0
    for blob in blobs:
        file_name = blob.name.split('/')[-1]
        match = old_format_regex.match(file_name)

        if match:
            ticker = match.group(1)
            new_name = f"{PREFIX}{ticker}_recommendation_{DEFAULT_DATE}.md"
            logging.info(f"Renaming '{blob.name}' to '{new_name}'")
            
            # Rename by copying to a new blob and deleting the old one
            new_blob = bucket.copy_blob(blob, bucket, new_name)
            blob.delete()
            count += 1
            
    logging.info(f"Finished. Renamed {count} files.")

if __name__ == "__main__":
    rename_old_recommendations()