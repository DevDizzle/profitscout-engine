import os
import logging
from collections import defaultdict
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
PROJECT_ID = "profitscout-lx6bb"
BUCKET_NAME = "profit-scout-data"
PREFIX = "earnings-call-transcripts/"

def cleanup_transcripts():
    """
    Scans the transcripts folder, groups files by ticker, and deletes all but the most recent one.
    """
    logging.info(f"Connecting to GCS bucket: {BUCKET_NAME}")
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    
    # List all blobs in the folder
    logging.info(f"Listing files in {PREFIX}...")
    blobs = list(bucket.list_blobs(prefix=PREFIX))
    
    if not blobs:
        logging.info("No files found.")
        return

    # Group by ticker
    # Format: TICKER_YYYY-MM-DD.json
    # Logic: Date is last 10 chars before .json
    files_by_ticker = defaultdict(list)
    
    for blob in blobs:
        filename = blob.name.split('/')[-1]
        if not filename.endswith('.json'):
            continue
            
        # Parse logic
        # AAL_2025-10-23.json
        # Date is usually at the end.
        try:
            # Assumes format TICKER_YYYY-MM-DD.json
            # Extract date string (last 10 chars before .json)
            date_part = filename[-15:-5]
            # Extract ticker (everything before _date_part)
            # This handles tickers with underscores gracefully if we just slice
            ticker = filename[:-16]
            
            files_by_ticker[ticker].append({
                'blob': blob,
                'date': date_part,
                'name': filename
            })
        except Exception as e:
            logging.warning(f"Could not parse filename {filename}: {e}")
            continue

    logging.info(f"Found files for {len(files_by_ticker)} tickers.")
    
    total_deleted = 0
    
    # Process groups
    for ticker, file_list in files_by_ticker.items():
        if len(file_list) > 1:
            # Sort by date descending (newest first)
            file_list.sort(key=lambda x: x['date'], reverse=True)
            
            # Keep the first one
            keep = file_list[0]
            delete_list = file_list[1:]
            
            logging.info(f"[{ticker}] Keeping {keep['name']} ({keep['date']})")
            
            for item in delete_list:
                logging.info(f"  -> Deleting OLD: {item['name']} ({item['date']})")
                try:
                    item['blob'].delete()
                    total_deleted += 1
                except Exception as e:
                    logging.error(f"Failed to delete {item['name']}: {e}")
        else:
            # logging.info(f"[{ticker}] Only 1 file found. Skipping.")
            pass

    logging.info(f"Cleanup complete. Deleted {total_deleted} old files.")

if __name__ == "__main__":
    cleanup_transcripts()
