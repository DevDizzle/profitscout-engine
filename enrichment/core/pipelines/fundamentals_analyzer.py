import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re

# --- THIS IS THE CORRECTED SECTION ---
METRICS_INPUT_PREFIX = config.PREFIXES["fundamentals_analyzer"]["input_metrics"]
RATIOS_INPUT_PREFIX = config.PREFIXES["fundamentals_analyzer"]["input_ratios"]
FUNDAMENTALS_OUTPUT_PREFIX = config.PREFIXES["fundamentals_analyzer"]["output"]

# One-shot example for consistent output format
_EXAMPLE_OUTPUT = """{
  "score": 0.68,
  "analysis": "The company's fundamentals present a compelling bullish case. Revenue per share shows consistent upward momentum over the past eight quarters, underscoring strong top-line growth. This is complemented by expanding profitability, as evidenced by a rising Return on Equity (ROE) and healthy gross profit margins, which have remained stable even as revenue has scaled. From a valuation standpoint, while the Price-to-Earnings (PE) ratio is elevated, it is justified by the strong growth profile and has shown signs of stabilizing. The balance sheet appears robust, with a manageable debt-to-equity ratio that has been steadily declining. Liquidity is also strong, with a current ratio consistently above 2.0, indicating ample capacity to cover short-term liabilities. The combination of strong growth, high profitability, and a solid financial position suggests a favorable risk-reward profile for the upcoming 6-12 months."
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

    # Read content from both input files
    metrics_content = gcs.read_blob(config.GCS_BUCKET_NAME, metrics_blob_path)
    ratios_content = gcs.read_blob(config.GCS_BUCKET_NAME, ratios_blob_path)

    if not metrics_content or not ratios_content:
        logging.error(f"[{ticker}] Missing metrics or ratios data for {date_str}. Skipping.")
        return None

    # Construct the unified prompt
    prompt = r"""You are a seasoned equity analyst evaluating a company’s key metrics and financial ratios to assess its investment attractiveness for the next 6-12 months.
Use **only** the JSON data provided.

### Key Interpretation Guidelines
1.  **Holistic View**: Synthesize insights from both metrics and ratios. For example, if PE ratios are high (from metrics), is it justified by strong ROE and margin expansion (from ratios)?
2.  **Profitability & Growth**: Analyze revenue growth, margins, and returns (ROE, ROIC).
3.  **Valuation**: Assess valuation multiples in the context of profitability and leverage.
4.  **Leverage & Liquidity**: Evaluate the balance sheet strength using debt ratios and current/quick ratios.
5.  **No Material Signals**: If the data is balanced or neutral, output a score of 0.50.

### Example Output (for format only; do not copy values or wording)
EXAMPLE_OUTPUT:
{{example_output}}

### Step-by-Step Reasoning
1.  Analyze trends in key metrics (e.g., revenue per share, cash flow).
2.  Analyze trends in financial ratios (e.g., profitability, liquidity, debt).
3.  Synthesize the findings to form a cohesive view of the company's fundamental health.
4.  Map the net result to probability bands:
    -   0.00-0.30 - clearly bearish
    -   0.31-0.49 - mildly bearish
    -   0.50       - neutral / balanced
    -   0.51-0.69 - moderately bullish
    -   0.70-1.00 - strongly bullish
5.  Summarize the most critical factors in one dense paragraph.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (200-400 words) summarizing the company's fundamental health, combining insights from both its key metrics and financial ratios.>"
}

### Provided Data

**Key Metrics:**
{key_metrics_data}

**Financial Ratios:**
{ratios_data}
""".replace("{{key_metrics_data}}", metrics_content).replace("{{ratios_data}}", ratios_content).replace("{{example_output}}", _EXAMPLE_OUTPUT)

    # Generate analysis and write to the new location
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

    # List all available input files from both sources
    all_metrics_files = {os.path.basename(p): p for p in gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=METRICS_INPUT_PREFIX)}
    all_ratios_files = {os.path.basename(p): p for p in gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=RATIOS_INPUT_PREFIX)}
    all_analyses = set(os.path.basename(p) for p in gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=FUNDAMENTALS_OUTPUT_PREFIX))

    work_items = []
    # Iterate through metrics files to find corresponding ratios files
    for file_name in all_metrics_files:
        if file_name in all_ratios_files and file_name not in all_analyses:
            ticker, date_str = parse_filename(file_name)
            if ticker and date_str:
                work_items.append((ticker, date_str))

    if not work_items:
        logging.info("All fundamental analyses are up-to-date.")
        return

    logging.info(f"Found {len(work_items)} new sets of fundamentals to analyze.")

    # Process the missing items in parallel
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_fundamental_files, ticker, date_str) for ticker, date_str in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())

    logging.info(f"--- Fundamentals Analysis Pipeline Finished. Processed {count} new files. ---")