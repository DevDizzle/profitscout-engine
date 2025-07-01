import logging
from datetime import datetime
from google.cloud import storage

# --- Configuration ---
# Please confirm these values are correct before running.
PROJECT_ID = "profitscout-lx6bb"
GCS_BUCKET_NAME = "profit-scout-data"
SUMMARIES_PREFIX = "earnings-call-summaries/"
DATE_TO_DELETE = "2025-07-01" # Format: YYYY-MM-DD

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def delete_summaries_by_date():
    """
    Finds and deletes summary files in GCS based on their CREATION DATE,
    with a user confirmation step.
    """
    try:
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        
        logging.info(f"Scanning gs://{GCS_BUCKET_NAME}/{SUMMARIES_PREFIX} for files created on '{DATE_TO_DELETE}'...")

        # Parse the target date string into a date object
        target_date = datetime.strptime(DATE_TO_DELETE, "%Y-%m-%d").date()

        # --- FIXED LOGIC ---
        # Find all blobs and filter by their 'time_created' metadata attribute
        all_blobs = bucket.list_blobs(prefix=SUMMARIES_PREFIX)
        blobs_to_delete = []
        for blob in all_blobs:
            # The blob.time_created is timezone-aware (UTC), so we get just the .date() part
            if blob.time_created.date() == target_date:
                blobs_to_delete.append(blob)

        if not blobs_to_delete:
            logging.info("No matching summary files found to delete.")
            return

        logging.warning(f"Found {len(blobs_to_delete)} files to delete:")
        for blob in blobs_to_delete:
            print(f"  - {blob.name} (Created: {blob.time_created.date()})")

        # --- SAFETY CONFIRMATION STEP ---
        confirm = input("\nAre you sure you want to permanently delete these files? (yes/no): ")

        if confirm.lower() == 'yes':
            logging.info("User confirmed. Deleting files...")
            for blob in blobs_to_delete:
                logging.info(f"Deleting {blob.name}...")
                blob.delete()
            logging.info(f"\nSuccessfully deleted {len(blobs_to_delete)} files.")
        else:
            logging.info("Deletion cancelled by user.")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    delete_summaries_by_date()