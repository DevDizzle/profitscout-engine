import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re
import json

# Define prefixes from the central configuration
INPUT_PREFIX = config.PREFIXES["business_summarizer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["business_summarizer"]["output"]

# --- MODIFIED: A more concise one-shot example ---
_EXAMPLE_OUTPUT = """{
  "summary": "Apple Inc. designs and sells iconic consumer electronics, software, and online services. Its main products include the iPhone, Mac, and iPad, serving a global consumer and business market."
}"""

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

    # --- MODIFIED: Updated prompt for a shorter, SEO-friendly summary ---
    prompt = r"""You are an SEO expert creating a meta description. Read the "Business" section from a 10-K filing and create a dense, SEO-friendly summary of 100-200 characters.

Focus on:
1.  What the company's core business is (products/services).
2.  Who its primary customers are.

Do not include financial figures, forward-looking statements, or opinions.

Your output must be a single, clean JSON object with one key: "summary". Do not include any other text, explanations, or markdown.

### Example Output (for format only)
{{example_output}}

### Provided Business Section:
{{business_content}}
""".replace("{business_content}", business_content).replace("{example_output}", _EXAMPLE_OUTPUT)

    summary_json = vertex_ai.generate(prompt)
    if summary_json:
        gcs.write_text(config.GCS_BUCKET_NAME, summary_blob_path, summary_json, "application/json")
        gcs.cleanup_old_files(config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, summary_blob_path)
        return summary_blob_path
    return None

def run_pipeline():
    """Finds and processes business profiles that haven't been summarized."""
    logging.info("--- Starting Business Profile Summarizer Pipeline ---")
    all_profiles = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    all_summaries = set(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX))

    work_items = [
        p for p in all_profiles
        if f"{OUTPUT_PREFIX}{os.path.basename(p)}" not in all_summaries
    ]

    if not work_items:
        logging.info("All business profiles are already summarized.")
        return

    logging.info(f"Found {len(work_items)} new business profiles to summarize.")
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- Business Profile Summarizer Finished. Processed {count} new files. ---")