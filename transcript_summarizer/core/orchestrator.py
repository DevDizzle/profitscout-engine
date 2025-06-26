import logging
import re
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import MAX_THREADS, GCS_BUCKET_NAME, GCS_INPUT_PREFIX, GCS_OUTPUT_PREFIX, TICKER_LIST_PATH, TRANSCRIPT_SUMMARY_PROMPT
from core.gcs import get_tickers, list_blobs_for_tickers, blob_exists, download_transcript_text
from core.client import GeminiClient
from google.cloud import storage

def summarize_transcript(ticker: str, blob_name: str, genai_client: GeminiClient, bucket: storage.Bucket):
    """Processes a single transcript file."""
    try:
        blob = bucket.blob(blob_name)
        text = download_transcript_text(blob)

        filename = os.path.basename(blob_name)
        m = re.search(r"_(\d{4})-(\d{2})-(\d{2})\.json$", filename)
        if not m:
            logging.error(f"Could not parse date from filename: {filename}")
            return
        year, month = int(m.group(1)), int(m.group(2))
        quarter = (month - 1) // 3 + 1

        prompt = (
            TRANSCRIPT_SUMMARY_PROMPT.format(ticker=ticker, quarter=quarter, year=year)
            + text
        )

        summary = genai_client.summarize(prompt)
        if not summary:
            logging.error(f"Failed to generate summary for {filename}")
            return

        target_blob_name = f"{GCS_OUTPUT_PREFIX}{filename.replace('.json', '.txt')}"
        target_blob = bucket.blob(target_blob_name)
        
        with tempfile.NamedTemporaryFile(mode="w", delete=True, encoding="utf-8") as tmp:
            tmp.write(summary)
            tmp.flush()
            target_blob.upload_from_filename(tmp.name)

        logging.info(f"Uploaded summary → {target_blob_name}")
    except Exception as exc:
        logging.error(f"{ticker}: summarization failed – {exc}", exc_info=True)

def run_pipeline(genai_client: GeminiClient, storage_client: storage.Client, bucket: storage.Bucket) -> str:
    """Runs the full transcript summarization pipeline."""
    tickers_to_process = get_tickers(bucket)
    if not tickers_to_process:
        logging.warning("No tickers found in tickerlist.txt. Exiting.")
        return "No tickers to process"

    all_blobs = list_blobs_for_tickers(storage_client, bucket)
    logging.info(f"Found {len(all_blobs)} total transcripts in GCS. Filtering based on tickerlist.txt.")
    
    tasks = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        for b in all_blobs:
            if not b.name.endswith(".json"):
                continue

            try:
                ticker = os.path.basename(b.name).split("_")[0].upper()
            except IndexError:
                continue

            if ticker not in tickers_to_process:
                continue
            
            out_blob_name = b.name.replace(GCS_INPUT_PREFIX, GCS_OUTPUT_PREFIX).replace(".json", ".txt")
            if blob_exists(bucket, out_blob_name):
                logging.info(f"{ticker}: summary already exists; skipping")
                continue

            logging.info(f"Queueing summarization for {ticker} from {b.name}")
            tasks.append(pool.submit(summarize_transcript, ticker, b.name, genai_client, bucket))

        logging.info(f"Processing {len(tasks)} new summaries.")
        for f in as_completed(tasks):
            f.result()

    logging.info("All transcripts processed.")
    return "Summaries refreshed"