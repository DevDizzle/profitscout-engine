import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re

INPUT_PREFIX = config.PREFIXES["mda_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["mda_analyzer"]["output"]

# --- MODIFIED: A much more concise one-shot example ---
_EXAMPLE_OUTPUT = """{
  "score": 0.62,
  "analysis": "AbbVie's MD&A points to a moderately bullish outlook. Strong revenue growth from new products like Skyrizi and Rinvoq is effectively offsetting the expected decline in Humira sales. Positive operating leverage is demonstrated by increased gross margins and lower SG&A costs. While the pipeline has seen some setbacks, strong cash flow provides flexibility for continued R&D and shareholder returns. Key risks remain from biosimilar competition and macroeconomic pressures, but the company's diversified portfolio provides a solid foundation."
}"""

def parse_filename(blob_name: str):
    pattern = re.compile(r"([A-Z.]+)_(\\d{4}-\\d{2}-\\d{2})\\.txt$")
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
    
    # --- MODIFIED: Updated prompt for a shorter, more direct analysis ---
    prompt = r"""You are a sharp financial analyst evaluating a company’s Management’s Discussion & Analysis (MD&A) summary to find signals that may influence the stock over the next 1-3 months.
Use **only** the narrative supplied.

### Key Interpretation Guidelines
1.  **Growth & Profitability**: Is revenue and margin expansion accelerating or contracting?
2.  **Liquidity & Cash Flow**: Is cash flow strong and the balance sheet healthy?
3.  **Outlook Tone**: Does management sound confident or cautious about the future?
4.  **No Material Signals**: If balanced, output 0.50 and state fundamentals are neutral.

### Example Output (for format only; do not copy values or wording)
{{example_output}}

### Step-by-Step Reasoning
1.  Classify datapoints as bullish, bearish, or neutral.
2.  Map the net result to probability bands:
    -   0.00-0.30 - clearly bearish
    -   0.31-0.49 - mildly bearish
    -   0.50       - neutral / balanced
    -   0.51-0.69 - moderately bullish
    -   0.70-1.00 - strongly bullish
3.  Summarize decisive positives/negatives in one dense paragraph.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (150-200 words) summarizing key bullish vs bearish factors, balance-sheet strength, cash-flow trajectory, and management outlook.>"
}

Provided data:
{{mda_summary}}
""".replace("{{mda_summary}}", summary_content).replace("{{example_output}}", _EXAMPLE_OUTPUT)

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