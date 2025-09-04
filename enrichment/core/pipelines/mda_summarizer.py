import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re
from datetime import datetime
import json

INPUT_PREFIX = config.PREFIXES["mda_summarizer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["mda_summarizer"]["output"]

def parse_filename(blob_name: str):
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    if not match:
        return None, None, None, None, None
    ticker, date_str = match.groups()
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        quarter = (date_obj.month - 1) // 3 + 1
        year = date_obj.year
        filing_type = "10-Q" if date_obj.month in [3, 6, 9] else "10-K"
        return ticker, date_str, filing_type, year, quarter
    except ValueError:
        return None, None, None, None, None

def read_mda_data(raw_json: str):
    try:
        return json.loads(raw_json).get("mda")
    except (json.JSONDecodeError, TypeError):
        return None

def process_blob(blob_name: str):
    ticker, date_str, filing_type, year, quarter = parse_filename(blob_name)
    if not all([ticker, date_str, filing_type, year, quarter]):
        return None
    
    summary_blob_path = f"{OUTPUT_PREFIX}{ticker}_{date_str}.txt"
    logging.info(f"[{ticker}] Generating MD&A summary for {date_str}")
    
    raw_json = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    mda_content = read_mda_data(raw_json)
    if not mda_content:
        return None

    # --- MODIFIED: A much more concise and focused prompt ---
    mda_prompt = r"""You are a financial analyst tasked with creating a concise, easy-to-read summary of a company's MD&A (Management's Discussion & Analysis).
Your summary will be used by another AI for analysis, so it must be dense and informative.

Use only the provided MD&A text.

### Core Task
Summarize the most critical information regarding:
1.  **Results of Operations**: What drove the changes in revenue and profit?
2.  **Liquidity & Cash Flow**: Is the company's financial position strong or weak? How is its cash flow?
3.  **Outlook & Guidance**: What is management's tone and what guidance was provided?

### Final Output Instructions
- Produce one continuous, dense narrative of **150-250 words**.
- Do NOT include any topic keywords.
- Integrate specific figures and percentages where they are most impactful.

Input MD&A text for {ticker} (Q{quarter} {year}):
{mda}
""".format(
        ticker=ticker,
        quarter=quarter,
        year=year,
        mda=mda_content,
    )

    summary_text = vertex_ai.generate(mda_prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, summary_blob_path, summary_text)
    gcs.cleanup_old_files(config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, summary_blob_path)
    return summary_blob_path

def run_pipeline():
    logging.info("--- Starting MD&A Summarizer Pipeline ---")
    all_mda_blobs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    all_summaries = set(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX))
    
    work_items = []
    for blob_name in all_mda_blobs:
        if not blob_name.endswith(".json"):
            continue
        ticker, date_str, _, _, _ = parse_filename(blob_name)
        if not ticker or not date_str:
            continue
        if f"{OUTPUT_PREFIX}{ticker}_{date_str}.txt" not in all_summaries:
            work_items.append(blob_name)
            
    if not work_items:
        logging.info("All MD&A summaries are up-to-date.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- MD&A Summarizer Pipeline Finished. Processed {count} new files. ---")