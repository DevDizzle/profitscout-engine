import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re

INPUT_PREFIX = config.PREFIXES["ratios_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["ratios_analyzer"]["output"]

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_2025-06-30.json'."""
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)

def process_blob(blob_name: str):
    """Processes one ratios file."""
    ticker, date_str = parse_filename(blob_name)
    if not ticker or not date_str:
        return None
    
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_{date_str}.json"
    logging.info(f"[{ticker}] Generating ratios analysis for {date_str}")
    
    content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    if not content:
        return None
    
    prompt = r"""You are a seasoned fundamental analyst evaluating a company’s **key ratios** over the last eight quarters to assess investment attractiveness for the next 6-12 months.
Use **only** the JSON provided — do **not** use external data or assumptions.

### Key Interpretation Guidelines
1. **Liquidity** - Improving current/quick/cash ratios are bullish; declines are bearish.
2. **Profitability & Margins** - Margin expansion and higher returns are bullish; contraction is bearish.
3. **Leverage & Solvency** - Lower debt and higher coverage are bullish; rising leverage is bearish.
4. **Efficiency** - Improved turnover ratios are bullish; worsening efficiency is bearish.
5. **Valuation Multiples** - Lower/contracting multiples with good fundamentals are bullish.
6. **Shareholder Policy** - Sustainable dividends and payouts are bullish; unsustainable is bearish.
7. **No Material Signals** - If flat and balanced, output 0.50 and state ratios are neutral.

### Step-by-Step Reasoning
1. Compute quarter-over-quarter and year-over-year changes for each ratio group where possible.
2. Classify each as bullish, bearish, or neutral.
3. Map net result to probability bands:
   - 0.00-0.30 - clearly bearish
   - 0.31-0.49 - mildly bearish
   - 0.50       - neutral / balanced
   - 0.51-0.69 - moderately bullish
   - 0.70-1.00 - strongly bullish
4. Summarize in one dense paragraph.

### Output — return exactly this JSON, nothing else
{{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (200-400 words) summarizing the most material improvements/deteriorations, liquidity & leverage context, valuation insight, and clear risk-reward verdict>"
}}

Provided data:
{{key_ratios_data}}
""".replace("{{key_ratios_data}}", content)

    analysis_json = vertex_ai.generate(prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, analysis_json, "application/json")
    gcs.cleanup_old_files(config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, analysis_blob_path)
    return analysis_blob_path

def run_pipeline():
    logging.info("--- Starting Ratios Analysis Pipeline ---")
    all_inputs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    all_analyses = set(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX))
    
    work_items = [
        blob for blob in all_inputs 
        if not f"{OUTPUT_PREFIX}{os.path.basename(blob)}" in all_analyses
    ]
            
    if not work_items:
        logging.info("All ratios analyses are up-to-date.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- Ratios Analysis Pipeline Finished. Processed {count} new files. ---")
