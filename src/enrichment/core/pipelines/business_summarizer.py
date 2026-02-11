# enrichment/core/pipelines/business_summarizer.py
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from .. import config, gcs
from ..clients import vertex_ai

INPUT_PREFIX = config.PREFIXES["business_summarizer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["business_summarizer"]["output"]


def parse_filename(blob_name: str):
    """Parses filenames like 'AAPL_2025-06-30.json'."""
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)


def read_business_data(raw_json: str):
    """Extracts the 'business' content from the input JSON."""
    try:
        return json.loads(raw_json).get("business")
    except (json.JSONDecodeError, TypeError):
        return None


def process_blob(blob_name: str):
    """Processes one SEC business section file."""
    ticker, date_str = parse_filename(blob_name)
    if not ticker or not date_str:
        return None

    summary_blob_path = f"{OUTPUT_PREFIX}{ticker}_{date_str}.json"
    logging.info(f"[{ticker}] Generating business profile summary for {date_str}")

    raw_json = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    business_content = read_business_data(raw_json)
    if not business_content:
        logging.error(f"[{ticker}] No business content found in {blob_name}")
        return None

    # Simplified Prompt
    prompt = f"""You are an expert financial analyst. Summarize the provided "Business" section from a 10-K filing into a dense, informative paragraph (150-250 words).

Focus on:
1. Core products and services.
2. Target markets and customers.
3. Key business strategies and differentiators.

Output strict JSON with a single key "summary".

Business Section:
{business_content}
"""

    try:
        summary_json = vertex_ai.generate(prompt, response_mime_type="application/json")
        if summary_json:
            gcs.write_text(
                config.GCS_BUCKET_NAME, summary_blob_path, summary_json, "application/json"
            )
            gcs.cleanup_old_files(
                config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, summary_blob_path
            )
            return summary_blob_path
    except Exception as e:
        logging.error(f"[{ticker}] Failed to generate summary: {e}", exc_info=True)

    return None


def run_pipeline():
    """Finds and processes business profiles that haven't been summarized."""
    logging.info("--- Starting Business Profile Summarizer Pipeline ---")
    all_profiles = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    all_summaries = set(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX))

    work_items = [
        p
        for p in all_profiles
        if f"{OUTPUT_PREFIX}{os.path.basename(p)}" not in all_summaries
    ]

    if not work_items:
        logging.info("All business profiles are already summarized.")
        return

    logging.info(f"Found {len(work_items)} new business profiles to summarize.")
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(
        f"--- Business Profile Summarizer Finished. Processed {count} new files. ---"
    )