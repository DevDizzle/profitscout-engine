import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re

INPUT_PREFIX = config.PREFIXES["financials_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["financials_analyzer"]["output"]

# --- MODIFIED: A more concise one-shot example ---
_EXAMPLE_OUTPUT = """{
  "score": 0.45,
  "analysis": "AAON's financials present a mixed outlook. While revenue is trending upwards, profitability and margins have been volatile. A key concern is the highly variable and recently negative operating and free cash flow. The balance sheet is weakening due to a substantial increase in total debt and rising inventory levels, suggesting potential leverage and sales conversion issues. Despite top-line growth, inconsistent cash flow and a heavier debt load raise questions about the company's near-term financial stability."
}"""

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_2025-06-30.json'."""
    pattern = re.compile(r"([A-Z.]+)_(\\d{4}-\\d{2}-\\d{2})\\.json$")
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
    
# --- MODIFIED: A data-driven one-shot example ---
_EXAMPLE_OUTPUT = """{
  "score": 0.45,
  "analysis": "AAON's financials present a mixed outlook. While revenue shows an upward trend from $262.1M in Q1 2024 to $311.6M in Q2 2025, profitability has been volatile, with gross margins peaking in Q3 2024 before declining. A key concern is the highly variable and recently negative operating cash flow, which registered at -$23.2M in the latest quarter. The balance sheet is weakening due to a substantial increase in total debt from $17.2M to $352M over the last year, suggesting a significant rise in leverage. Despite top-line growth, the inconsistent cash flow and heavier debt load raise questions about the company's near-term financial stability."
}"""

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
    
    # --- MODIFIED: Updated prompt to require specific data points ---
    prompt = r"""You are a sharp financial analyst evaluating quarterly financial data to assess a company's health and trajectory over the next 6-12 months.

Use **only** the JSON provided. Your analysis **must** be grounded in the data.

### Key Interpretation Guidelines
1.  **Growth**: Is revenue growing? Cite the revenue figures from the first and last quarters provided.
2.  **Profitability**: Are margins expanding or contracting? Reference the `grossProfitRatio`.
3.  **Cash Flow**: Is the company generating or burning cash? Cite the `operatingCashFlow` from the most recent quarter.
4.  **Solvency**: How is the company's debt changing? Cite the `totalDebt` from the first and last quarters.
5.  **Synthesis**: Combine these points into a cohesive narrative.

### Example Output (for format and tone; do not copy values)
{{example_output}}

### Step-by-Step Reasoning
1.  Compute and compare quarterly changes for key metrics (Revenue, Gross Margin, Operating Cash Flow, Total Debt).
2.  Classify each as bullish, bearish, or neutral, citing the specific numbers.
3.  Map net findings to probability bands:
    -   0.00-0.30 - clearly bearish
    -   0.31-0.49 - mildly bearish
    -   0.50       - neutral / balanced
    -   0.51-0.69 - moderately bullish
    -   0.70-1.00 - strongly bullish
4.  Summarize into one dense paragraph, integrating the specific data points you identified.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense, informative paragraph (150-250 words) that summarizes key trends and financial health, **integrating specific dollar amounts and percentages** from the provided data.>"
}

Provided data:
{{financial_data}}
""".replace("{{financial_data}}", content).replace("{{example_output}}", _EXAMPLE_OUTPUT)

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