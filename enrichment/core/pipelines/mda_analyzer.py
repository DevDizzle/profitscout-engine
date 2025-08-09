import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re

INPUT_PREFIX = config.PREFIXES["mda_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["mda_analyzer"]["output"]

def parse_filename(blob_name: str):
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.txt$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)

def process_blob(blob_name: str):
    ticker, date_str = parse_filename(blob_name)
    if not ticker or not date_str:
        return None
    
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_{date_str}.json"
    logging.info(f"[{ticker}] Generating MD&A analysis for {date_str}")
    
    summary_content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    if not summary_content:
        return None
    
    prompt = r"""You are a seasoned fundamental-analysis specialist evaluating a company’s **Management’s Discussion & Analysis (MD&A) summary** to judge how fundamentals may influence the stock over the next 1-3 months.
Use **only** the narrative supplied — do **not** import outside data, market prices, or assumptions.

### Key Interpretation Guidelines
1. **Growth & Profitability** - Revenue/margin expansion is bullish; contraction is bearish.
2. **Costs & Operating Leverage** - Rising costs are acceptable if revenue grows faster; margin pressure without offsetting growth is bearish.
3. **Liquidity & Cash Flow** - Strong operating/free cash flow and ample liquidity are bullish; negative cash flow or funding needs are bearish.
4. **Balance-Sheet Health** - Deleveraging and strong coverage ratios are bullish; rising leverage is bearish.
5. **Competitive/Cyclical Factors** - Market expansion or leadership is bullish; reliance on vulnerable geographies or cycles is bearish.
6. **Outlook Tone** - Raised/lifted guidance is bullish; cautious tone is bearish.
7. **No Material Signals** - If empty or balanced, output 0.50 and state fundamentals are neutral.

### Step-by-Step Reasoning
1. Classify each datapoint as bullish, bearish, or neutral.
2. Weight by materiality (revenue impact, cash flow, geographic exposure).
3. Map the net result to probability bands:
   - 0.00-0.30 - clearly bearish
   - 0.31-0.49 - mildly bearish
   - 0.50       - neutral / balanced
   - 0.51-0.69 - moderately bullish
   - 0.70-1.00 - strongly bullish
4. Summarize decisive positives/negatives in one dense paragraph.

### Output — return exactly this JSON, nothing else
{{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (~200-300 words) summarizing key bullish vs bearish factors, balance-sheet strength, cash-flow trajectory, and management outlook.>"
}}

Provided data:
{{mda_summary}}
""".replace("{{mda_summary}}", summary_content)

    analysis_json = vertex_ai.generate(prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, analysis_json, "application/json")
    gcs.cleanup_old_files(config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, analysis_blob_path)
    return analysis_blob_path

def run_pipeline():
    logging.info("--- Starting MD&A Analysis Pipeline ---")
    all_summaries = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    all_analyses = set(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX))
    
    work_items = [
        s for s in all_summaries 
        if not f"{OUTPUT_PREFIX}{os.path.basename(s).replace('.txt', '.json')}" in all_analyses
    ]
            
    if not work_items:
        logging.info("All MD&A analyses are up-to-date.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- MD&A Analysis Pipeline Finished. Processed {count} new files. ---")
