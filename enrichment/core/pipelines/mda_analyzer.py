import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re
import json

# The input is now the raw MD&A JSON, and the output is the final analysis
INPUT_PREFIX = config.PREFIXES["mda_summarizer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["mda_analyzer"]["output"]

# --- MODIFIED: A data-driven one-shot example based on the raw MD&A text ---
_EXAMPLE_OUTPUT = """{
  "score": 0.48,
  "analysis": "AAON's management discussion reveals a mixed operational picture. While the total backlog grew an impressive 53.1% to $995.3 million, driven by strong demand for BASX data center solutions, this has strained liquidity. Net sales for the quarter were flat, decreasing 0.6%, with strong BASX growth offset by an 18.0% decline in the AAON Oklahoma segment due to supply chain and ERP implementation issues. Gross profit margin saw a significant contraction from 36.1% to 26.6%, reflecting production challenges. A key concern is the negative operating cash flow of -$31.0 million for the first six months, a sharp reversal from a $127.9 million inflow in the prior year, highlighting the working capital needed to support the growing backlog."
}"""


def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_2025-06-30.json'."""
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)

def read_mda_data(raw_json: str):
    """Extracts the 'mda' content from the input JSON."""
    try:
        return json.loads(raw_json).get("mda")
    except (json.JSONDecodeError, TypeError):
        return None

def process_blob(blob_name: str):
    """Processes one raw MD&A file to generate a final analysis."""
    ticker, date_str = parse_filename(blob_name)
    if not ticker or not date_str:
        return None
    
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_{date_str}.json"
    logging.info(f"[{ticker}] Generating direct MD&A analysis for {date_str}")
    
    raw_json_content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    if not raw_json_content:
        return None

    mda_content = read_mda_data(raw_json_content)
    if not mda_content:
        logging.error(f"[{ticker}] No 'mda' key found in {blob_name}")
        return None
    
    # --- MODIFIED: New single-pass prompt for direct analysis ---
    prompt = r"""You are a sharp financial analyst evaluating a company’s Management’s Discussion & Analysis (MD&A) to find signals that may influence the stock over the next 1-3 months.

Use **only** the provided MD&A text. Your analysis **must** be grounded in the data.

### Core Task & Data Integration
Summarize and analyze the most critical information, citing specific figures:
1.  **Results of Operations**: How did total `Net Sales` change year-over-year for the three-month period? What was the `Gross Profit` margin for the current quarter versus the prior year?
2.  **Liquidity & Cash Flow**: What was the `Net cash (used in) provided by operating activities` for the six-month period?
3.  **Outlook & Guidance**: What is the qualitative outlook? Look for forward-looking statements, guidance, or commentary on market conditions (e.g., "strong demand," "slowing construction," "macroeconomic factors").
4.  **Synthesis**: Combine these data points into a cohesive narrative about the company's performance and outlook.

### Example Output (for format and tone; do not copy values)
{{example_output}}

### Step-by-Step Reasoning
1.  Identify and extract the specific data points required by the guidelines from the text.
2.  Analyze the trends and absolute values of these metrics.
3.  Map the net result to a probability score:
    -   0.00-0.30 → clearly bearish
    -   0.31-0.49 → mildly bearish
    -   0.50       → neutral / balanced
    -   0.51-0.69 → moderately bullish
    -   0.70-1.00 → strongly bullish
4.  Summarize the key factors into one dense paragraph, integrating the specific data points you identified.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (150-250 words) summarizing key factors, balance-sheet strength, cash-flow trajectory, and management outlook, **integrating specific figures** from the MD&A.>"
}

Provided MD&A text:
{{mda_content}}
""".replace("{{mda_content}}", mda_content).replace("{{example_output}}", _EXAMPLE_OUTPUT)

    analysis_json = vertex_ai.generate(prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, analysis_json, "application/json")
    gcs.cleanup_old_files(config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, analysis_blob_path)
    return analysis_blob_path

def run_pipeline():
    logging.info("--- Starting Direct MD&A Analysis Pipeline ---")
    all_inputs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    all_analyses = set(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX))
    
    work_items = [
        s for s in all_inputs 
        if not f"{OUTPUT_PREFIX}{os.path.basename(s)}" in all_analyses
    ]
            
    if not work_items:
        logging.info("All MD&A analyses are up-to-date.")
        return

    logging.info(f"Found {len(work_items)} new MD&A sections to analyze.")
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- MD&A Analysis Pipeline Finished. Processed {count} new files. ---")