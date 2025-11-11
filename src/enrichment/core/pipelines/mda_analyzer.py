# enrichment/core/pipelines/mda_analyzer.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
from .helpers import load_latest_macro_thesis
import os
import re
import json

# CORRECTED: The input is now pointed to its own configuration, not the old summarizer's.
INPUT_PREFIX = config.PREFIXES["mda_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["mda_analyzer"]["output"]

_EXAMPLE_OUTPUT = """{
  "score": 0.44,
  "analysis": "AAON's MD&A presents a guarded stance. Net sales for the quarter decreased 0.6% year over year while gross margin compressed from 36.1% to 26.6%, tying to ERP-driven inefficiencies. Operating cash flow for the six months swung to 'net cash used in operating activities of $31.0 million', signaling liquidity strain as backlog builds. Management's forward-looking remark that 'we expect supply chain friction to ease gradually through 2H25' tempers concerns but must be weighed against the macro baseline outlined above. Their liquidity caution mirrors elements of the counterpoint thesis, keeping the skew mildly bearish despite selective optimism."
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
    
    macro_thesis = load_latest_macro_thesis()
    prompt = r"""You are a sharp financial analyst evaluating a company’s Management’s Discussion & Analysis (MD&A) to find signals that may influence the stock over the next 1-3 months.
Use **only** the provided MD&A text. Your analysis **must** be grounded in the data.

### Macro Context (read carefully before analyzing)
- Baseline macro thesis: {macro_trend}
- Counterpoint / risks: {anti_thesis}

### Core Task & Data Integration
Summarize and analyze the most critical information, citing specific figures:
1.  **Results of Operations**: Quantify how total `Net Sales` changed year-over-year for the three-month period and compare the `Gross Profit` margin versus the prior year.
2.  **Liquidity & Cash Flow**: State the `Net cash (used in) provided by operating activities` for the six-month period and comment on balance-sheet flexibility.
3.  **Forward-Looking Signals**: Explicitly extract any forward-looking statements or guidance. Provide the single-quoted snippet(s) that capture management’s outlook or commitments.
4.  **Macro Alignment Check**: Evaluate whether management’s outlook reinforces, contradicts, or is insulated from the macro thesis above. Explain the alignment with both the baseline and counterpoint.
5.  **Synthesis**: Combine these data points into a cohesive narrative about the company's performance, liquidity, and outlook.

### CRITICAL FORMATTING RULE
- When including direct quotes or phrases in the 'analysis' text, you MUST use single quotes ('), not double quotes (").

### Example Output (for format and tone; do not copy values)
{{example_output}}

### Step-by-Step Reasoning
1.  Identify and extract the specific data points required by the guidelines from the MD&A.
2.  List the forward-looking statements or guidance verbatim (single-quoted) and note whether they are incremental or reaffirmations.
3.  Compare those statements to the macro baseline and counterpoint to determine alignment or tension.
4.  Map the net result to a probability score:
    -   0.00-0.30 → clearly bearish
    -   0.31-0.49 → mildly bearish
    -   0.50       → neutral / balanced
    -   0.51-0.69 → moderately bullish
    -   0.70-1.00 → strongly bullish
5.  Summarize the key factors into one dense paragraph, integrating the required figures and macro comparison.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (150-250 words) summarizing key factors, cash-flow trajectory, macro alignment, and management outlook, **integrating specific figures and forward-looking quotes** from the MD&A.>"
}

Provided MD&A text:
{{mda_content}}
""".replace("{{mda_content}}", mda_content)
    prompt = prompt.replace("{macro_trend}", macro_thesis["macro_trend"])
    prompt = prompt.replace("{anti_thesis}", macro_thesis["anti_thesis"])
    prompt = prompt.replace("{{example_output}}", _EXAMPLE_OUTPUT)

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