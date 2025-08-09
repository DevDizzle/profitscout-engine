import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re

INPUT_PREFIX = config.PREFIXES["financials_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["financials_analyzer"]["output"]

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_2025-06-30.json'."""
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)

def process_blob(blob_name: str):
    """Processes one financial statement file."""
    ticker, date_str = parse_filename(blob_name)
    if not ticker or not date_str:
        return None
    
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_{date_str}.json"
    logging.info(f"[{ticker}] Generating financials analysis for {date_str}")
    
    content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    if not content:
        return None
    
    prompt = r"""You are a seasoned financial analyst evaluating a company’s **quarterly financial data** to assess overall health and growth trajectory over the next 6-12 months.
Use **only** the JSON provided — do **not** use external data or assumptions.

### Key Interpretation Guidelines
1. **Growth** - Sustained revenue and profit growth are bullish; declines are bearish.
2. **Profitability** - Expanding margins and positive net income are bullish; contracting margins are bearish.
3. **Liquidity** - Strong cash/current ratios are bullish; weakening liquidity is bearish.
4. **Solvency** - Falling debt or rising coverage is bullish; rising leverage is bearish.
5. **Cash Flow** - Positive and rising free cash flow is bullish; persistent negative cash flow is bearish.
6. **Sustainability** - Identify whether trends are consistent or driven by one-off events.
7. **No Material Signals** - If flat and balanced, output 0.50 and state fundamentals appear neutral.

### Step-by-Step Reasoning
1. Compute and compare quarterly changes for key metrics.
2. Classify each as bullish, bearish, or neutral.
3. Weigh materiality and consistency across multiple quarters.
4. Map net findings to probability bands:
   - 0.00-0.30 - clearly bearish
   - 0.31-0.49 - mildly bearish
   - 0.50       - neutral / balanced
   - 0.51-0.69 - moderately bullish
   - 0.70-1.00 - strongly bullish
5. Summarize into one dense paragraph.

### Output — return exactly this JSON, nothing else
{{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (200-400 words) summarizing key trends, anomalies, and implications for financial health and trajectory.>"
}}

Provided data:
{{financial_data}}
""".replace("{{financial_data}}", content)

    analysis_json = vertex_ai.generate(prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, analysis_json, "application/json")
    gcs.cleanup_old_files(config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, analysis_blob_path)
    return analysis_blob_path

def run_pipeline():
    logging.info("--- Starting Financials Analysis Pipeline ---")
    all_inputs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    all_analyses = set(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX))
    
    work_items = [
        blob for blob in all_inputs 
        if not f"{OUTPUT_PREFIX}{os.path.basename(blob)}" in all_analyses
    ]
            
    if not work_items:
        logging.info("All financials analyses are up-to-date.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- Financials Analysis Pipeline Finished. Processed {count} new files. ---")
