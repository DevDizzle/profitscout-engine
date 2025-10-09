# src/utils/fetch_images.py
"""
Utility script to fetch company logos and upload them to Google Cloud Storage.

This script retrieves a list of stock tickers from a file in GCS, fetches
company profile information (including a logo URL) from the Financial
Modeling Prep (FMP) API, downloads the logo, and uploads it to a specified
GCS bucket.

Required Environment Variables:
- FMP_API_KEY: Your API key for the Financial Modeling Prep service.
- GCS_BUCKET_NAME: The name of the GCS bucket for storing logos. Defaults
  to 'profit-scout-data'.

The script processes tickers in batches to avoid overly long API requests and
logs the final GCS URI for each successfully processed logo to a local JSON file.
"""

import json
import logging
import os
from math import ceil

import requests
from google.cloud import storage

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Fetches configuration from environment variables.
FMP_API_KEY = os.getenv("FMP_API_KEY")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")
TICKER_LIST_PATH = "tickerlist.txt"
DESTINATION_FOLDER = "images/"
BATCH_SIZE = 50  # Number of tickers per API request.


def get_tickers_from_gcs(bucket_name: str, blob_path: str) -> list[str]:
    """
    Loads the official ticker list from a specified file in a GCS bucket.

    Args:
        bucket_name: The name of the GCS bucket.
        blob_path: The path to the ticker list file within the bucket.

    Returns:
        A list of uppercase stock tickers, or an empty list if an error occurs.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        if not blob.exists():
            logging.error(f"Ticker file not found in GCS: gs://{bucket_name}/{blob_path}")
            return []

        content = blob.download_as_text(encoding="utf-8")
        tickers = [
            line.strip().upper() for line in content.splitlines() if line.strip()
        ]
        logging.info(f"Successfully loaded {len(tickers)} tickers from GCS.")
        return tickers
    except Exception as e:
        logging.critical(f"Failed to load tickers from GCS: {e}", exc_info=True)
        return []


def upload_to_gcs(
    bucket_name: str, source_file_path: str, destination_blob_name: str
) -> str | None:
    """
    Uploads a local file to GCS and returns its GCS URI.

    Args:
        bucket_name: The name of the target GCS bucket.
        source_file_path: The path to the local file to upload.
        destination_blob_name: The desired blob name in the GCS bucket.

    Returns:
        The GCS URI (e.g., 'gs://bucket/path/to/blob') of the uploaded file,
        or None if the upload fails.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        return f"gs://{bucket_name}/{destination_blob_name}"
    except Exception as e:
        logging.error(
            f"Failed to upload {source_file_path} to GCS: {e}", exc_info=True
        )
        return None


def fetch_and_upload_logos(tickers: list[str]) -> dict[str, str]:
    """
    Fetches logos for a list of tickers and uploads them to GCS.

    Args:
        tickers: A list of stock tickers to process.

    Returns:
        A dictionary mapping tickers to their GCS logo URIs.
    """
    num_batches = ceil(len(tickers) / BATCH_SIZE)
    uri_map = {}
    logging.info(f"Starting to process {len(tickers)} tickers in {num_batches} batches.")

    for i in range(num_batches):
        batch_start = i * BATCH_SIZE
        batch_end = batch_start + BATCH_SIZE
        batch_tickers = tickers[batch_start:batch_end]
        tickers_str = ",".join(batch_tickers)

        logging.info(f"--- Processing Batch {i + 1}/{num_batches} ---")

        api_url = f"https://financialmodelingprep.com/api/v3/profile/{tickers_str}?apikey={FMP_API_KEY}"
        try:
            response = requests.get(api_url, timeout=20)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            logging.error(f"API request failed for batch {i + 1}: {e}")
            continue

        for item in data:
            ticker = item.get("symbol")
            logo_url = item.get("image")

            if not ticker or not logo_url:
                logging.warning(
                    f"Skipping item due to missing ticker or logo URL: {item}"
                )
                continue

            local_file = ""
            try:
                img_response = requests.get(logo_url, timeout=20)
                img_response.raise_for_status()

                ext = os.path.splitext(logo_url)[1] or ".png"
                local_file = f"{ticker}{ext}"

                with open(local_file, "wb") as f:
                    f.write(img_response.content)

                blob_name = f"{DESTINATION_FOLDER}{ticker}{ext}"
                gcs_uri = upload_to_gcs(GCS_BUCKET_NAME, local_file, blob_name)

                if gcs_uri:
                    uri_map[ticker] = gcs_uri
                    logging.info(f"Successfully uploaded logo for {ticker}: {gcs_uri}")

            except requests.RequestException as e:
                logging.error(f"Failed to download logo for {ticker}: {e}")
            finally:
                if os.path.exists(local_file):
                    os.remove(local_file)

    return uri_map


def main():
    """Main function to orchestrate the fetching and uploading of company logos."""
    if not FMP_API_KEY:
        logging.critical(
            "FMP_API_KEY environment variable not set. Please set it before running."
        )
        return

    tickers = get_tickers_from_gcs(GCS_BUCKET_NAME, TICKER_LIST_PATH)
    if not tickers:
        logging.critical("No tickers loaded. Exiting.")
        return

    uri_map = fetch_and_upload_logos(tickers)

    if uri_map:
        output_filename = "ticker_uris.json"
        with open(output_filename, "w") as f:
            json.dump(uri_map, f, indent=4)
        logging.info(
            f"Saved URI map for {len(uri_map)} tickers to {output_filename}."
        )

    logging.info("--- Image fetching process complete! ---")


if __name__ == "__main__":
    main()