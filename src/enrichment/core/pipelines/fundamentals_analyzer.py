import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re

METRICS_INPUT_PREFIX = config.PREFIXES["fundamentals_analyzer"]["input_metrics"]
RATIOS_INPUT_PREFIX = config.PREFIXES["fundamentals_analyzer"]["input_ratios"]
FUNDAMENTALS_OUTPUT_PREFIX = config.PREFIXES["fundamentals_analyzer"]["output"]

# --- MODIFIED: A more concise one-shot example ---
_EXAMPLE_OUTPUT = """{
  "score": 0.72,
  "analysis": "The company exhibits a strong bullish fundamental profile. Consistent revenue growth is supported by expanding profitability, highlighted by a rising Return on Equity (ROE) and stable gross margins. While the P/E ratio is elevated, it appears justified by the growth trajectory. The balance sheet is robust, with a declining debt-to-equity ratio and strong liquidity, indicated by a current ratio above 2.0. This combination of growth, profitability, and financial strength points to a favorable outlook."
}"""

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_2025-06-30.json'."""
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)

def process_fundamental_files(ticker: str, date_str: str):
    """
    Processes a pair of key metrics and ratios files for a single ticker and date.
    """
    metrics_blob_path = f"{METRICS_INPUT_PREFIX}{ticker}_{date_str}.json"
    ratios_blob_path = f"{RATIOS_INPUT_PREFIX}{ticker}_{date_str}.json"
    analysis_blob_path = f"{FUNDAMENTALS_OUTPUT_PREFIX}{ticker}_{date_str}.json"

    logging.info(f"[{ticker}] Generating combined fundamentals analysis for {date_str}")

    metrics_content = gcs.read_blob(config.GCS_BUCKET_NAME, metrics_blob_path)
    ratios_content = gcs.read_blob(config.GCS_BUCKET_NAME, ratios_blob_path)

    if not metrics_content or not ratios_content:
        logging.error(f"[{ticker}] Missing metrics or ratios data for {date_str}. Skipping.")
        return None

    # --- MODIFIED: Updated prompt for a shorter, more direct analysis ---
    prompt = r"""You are a sharp equity analyst writing for a fast-paced audience. Evaluate the company’s key metrics and financial ratios to assess its investment attractiveness for the next 6-12 months.
Use **only** the JSON data provided.

### Key Interpretation Guidelines
1.  **Holistic View**: Synthesize insights from both metrics and ratios.
2.  **Profitability & Growth**: Analyze revenue, margins, and returns.
3.  **Valuation**: Assess valuation multiples in context.
4.  **Leverage & Liquidity**: Evaluate balance sheet strength.
5.  **No Material Signals**: If balanced, output a score of 0.50.

### Example Output (for format only; do not copy values or wording)
{{example_output}}

### Step-by-Step Reasoning
1.  Analyze trends in key metrics and financial ratios.
2.  Synthesize the findings into a cohesive view.
3.  Map the net result to probability bands:
    -   0.00-0.30 - clearly bearish
    -   0.31-0.49 - mildly bearish
    -   0.50       - neutral / balanced
    -   0.51-0.69 - moderately bullish
    -   0.70-1.00 - strongly bullish
4.  Summarize the most critical factors in one dense paragraph.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (150-250 words) summarizing the company's fundamental health, combining insights from its key metrics and financial ratios.>"
}

### Provided Data

**Key Metrics:**
{key_metrics_data}

**Financial Ratios:**
{ratios_data}
""".replace("{{key_metrics_data}}", metrics_content).replace("{{ratios_data}}", ratios_content).replace("{{example_output}}", _EXAMPLE_OUTPUT)

    analysis_json = vertex_ai.generate(prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, analysis_json, "application/json")
    gcs.cleanup_old_files(config.GCS_BUCKET_NAME, FUNDAMENTALS_OUTPUT_PREFIX, ticker, analysis_blob_path)

    return analysis_blob_path

def run_pipeline():
    """
    Finds and processes pairs of key metrics and ratios files that have not yet
    been analyzed by the combined fundamentals_analyzer.
    """
    logging.info("--- Starting Combined Fundamentals Analysis Pipeline ---")

    all_metrics_files = {os.path.basename(p): p for p in gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=METRICS_INPUT_PREFIX)}
    all_ratios_files = {os.path.basename(p): p for p in gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=RATIOS_INPUT_PREFIX)}
    all_analyses = set(os.path.basename(p) for p in gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=FUNDAMENTALS_OUTPUT_PREFIX))

    work_items = []
    for file_name in all_metrics_files:
        if file_name in all_ratios_files and file_name not in all_analyses:
            ticker, date_str = parse_filename(file_name)
            if ticker and date_str:
                work_items.append((ticker, date_str))

    if not work_items:
        logging.info("All fundamental analyses are up-to-date.")
        return

    logging.info(f"Found {len(work_items)} new sets of fundamentals to analyze.")

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_fundamental_files, ticker, date_str) for ticker, date_str in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())

    logging.info(f"--- Fundamentals Analysis Pipeline Finished. Processed {count} new files. ---")