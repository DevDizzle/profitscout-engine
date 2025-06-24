# transcript_summarizer/core/orchestrator.py
import logging
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import (
    MAX_WORKERS, GCS_BUCKET_NAME, GCS_INPUT_FOLDER, GCS_OUTPUT_FOLDER,
    TRANSCRIPT_SUMMARY_PROMPT, MODEL_NAME, TEMPERATURE, MAX_TOKENS
)
from core.gcs import list_input_blobs, blob_exists, download_transcript_text, upload_summary
from core.client import GeminiClient
from google.cloud import storage

def process_transcript(blob: storage.Blob, gemini_client: GeminiClient, storage_client: storage.Client):
    """Orchestrates the summarization for a single transcript blob."""
    filename = os.path.basename(blob.name)
    if not filename.endswith(".json"):
        return f"Skipping non-JSON file: {filename}"

    # Construct the expected output path
    output_filename = filename.replace(".json", ".txt")
    output_path = os.path.join(GCS_OUTPUT_FOLDER, output_filename)

    if blob_exists(storage_client, GCS_BUCKET_NAME, output_path):
        return f"Summary already exists for {filename}, skipping."

    # Parse metadata from the filename (e.g., 'AAPL_2025-04-23.json')
    match = re.search(r"([A-Z]+)_(\d{4})-(\d{2})-(\d{2})\.json$", filename)
    if not match:
        return f"Could not parse ticker/date from filename: {filename}"
    
    ticker, year_str, month_str, _ = match.groups()
    year, month = int(year_str), int(month_str)
    quarter = (month - 1) // 3 + 1

    logging.info(f"Processing transcript for {ticker} Q{quarter} {year}...")
    transcript_text = download_transcript_text(blob)
    if not transcript_text:
        return f"No content found in transcript: {filename}"

    # Format the final prompt
    prompt = TRANSCRIPT_SUMMARY_PROMPT.format(
        ticker=ticker,
        quarter=quarter,
        year=year,
        transcript_text=transcript_text
    )

    summary = gemini_client.summarize(prompt, MODEL_NAME, TEMPERATURE, MAX_TOKENS)

    if not summary:
        return f"Failed to generate summary for {filename}."

    upload_summary(storage_client, GCS_BUCKET_NAME, summary, output_path)
    return f"Successfully created and uploaded summary for {filename}."

def run_pipeline(gemini_client: GeminiClient, storage_client: storage.Client):
    """Runs the full transcript summarization pipeline."""
    input_blobs = list_input_blobs(storage_client, GCS_BUCKET_NAME, GCS_INPUT_FOLDER)
    if not input_blobs:
        logging.info("No transcripts found to summarize.")
        return

    logging.info(f"Found {len(input_blobs)} transcripts to process.")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_transcript, blob, gemini_client, storage_client): blob.name for blob in input_blobs}
        for future in as_completed(futures):
            try:
                result = future.result()
                logging.info(result)
            except Exception as e:
                blob_name = futures[future]
                logging.error(f"An error occurred processing {blob_name}: {e}", exc_info=True)
    
    logging.info("Transcript summarization pipeline complete.")