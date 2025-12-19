# enrichment/core/pipelines/fundamentals_analyzer.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re
import json

METRICS_INPUT_PREFIX = config.PREFIXES["fundamentals_analyzer"]["input_metrics"]
RATIOS_INPUT_PREFIX = config.PREFIXES["fundamentals_analyzer"]["input_ratios"]
FUNDAMENTALS_OUTPUT_PREFIX = config.PREFIXES["fundamentals_analyzer"]["output"]

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_2025-06-30.json'."""
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)

def _filter_recent_data(json_content: str, periods: int = 5) -> list:
    """
    Parses JSON and returns only the most recent 'periods' items.
    Assumes FMP data is sorted descending by date (newest first).
    """
    if not json_content:
        return []
    try:
        data = json.loads(json_content)
        if isinstance(data, list):
            # Take the top N items (Current + Trend history)
            return data[:periods]
        return []
    except json.JSONDecodeError:
        return []

def process_fundamental_files(ticker: str, date_str: str):
    """
    Processes a pair of key metrics and ratios files for a single ticker and date.
    """
    metrics_blob_path = f"{METRICS_INPUT_PREFIX}{ticker}_{date_str}.json"
    ratios_blob_path = f"{RATIOS_INPUT_PREFIX}{ticker}_{date_str}.json"
    analysis_blob_path = f"{FUNDAMENTALS_OUTPUT_PREFIX}{ticker}_{date_str}.json"

    logging.info(f"[{ticker}] Generating combined fundamentals analysis for {date_str}")

    metrics_raw = gcs.read_blob(config.GCS_BUCKET_NAME, metrics_blob_path)
    ratios_raw = gcs.read_blob(config.GCS_BUCKET_NAME, ratios_blob_path)

    if not metrics_raw or not ratios_raw:
        logging.error(f"[{ticker}] Missing metrics or ratios data for {date_str}. Skipping.")
        return None

    # --- FILTERING: Keep only the last 5 quarters to reduce noise ---
    # This prevents "time travel" hallucinations by limiting the context window.
    recent_metrics = _filter_recent_data(metrics_raw, periods=5)
    recent_ratios = _filter_recent_data(ratios_raw, periods=5)

    if not recent_metrics or not recent_ratios:
        logging.warning(f"[{ticker}] Data files were empty or invalid JSON.")
        return None

    # --- UPDATED PROMPT: Directs the AI to use both datasets effectively ---
    prompt = r"""
You are a sharp equity analyst. Evaluate the company’s Key Metrics and Financial Ratios to assess its investment attractiveness for the next 6-12 months.

### ANALYSIS DATE: {date_str}
Treat the data record with date `{date_str}` as the **CURRENT** period. The other records are historical context for identifying trends.

### Data Provided
- **Key Metrics (Last 5 Quarters):** Use this for **Valuation** (PE, EV/EBITDA) and **Per-Share Growth** (Revenue/FCF per share).
{recent_metrics}

- **Financial Ratios (Last 5 Quarters):** Use this for **Efficiency** (Margins, ROE) and **Liquidity** (Current Ratio).
{recent_ratios}

### Core Tasks
1.  **Growth & Valuation**: Are `revenuePerShare` and `freeCashFlowPerShare` growing? Is the valuation (`peRatio`, `priceToSalesRatio`) expanding or contracting?
2.  **Operational Efficiency**: Check `grossProfitMargin` and `operatingProfitMargin`. Are margins improving or deteriorating?
3.  **Financial Health**: Check the `debtToEquity` and `currentRatio`. Is the balance sheet getting stronger or weaker?
4.  **Trend Verdict**: Compare the CURRENT period to the same period 1 year ago (4 quarters prior). Is the business accelerating or slowing down?

### Scoring
- **0.0 - 0.3 (Bearish):** Deteriorating fundamentals (falling margins, shrinking revenue, rising debt).
- **0.4 - 0.6 (Neutral):** Mixed signals (e.g., cheap valuation but falling growth) or stagnation.
- **0.7 - 1.0 (Bullish):** Improving fundamentals (expanding margins, growth, healthy balance sheet).

### Output — return exactly this JSON
{{
  "score": <float between 0.0 and 1.0>,
  "analysis": "<One dense paragraph (150-250 words). Start by stating the clear fundamental trend (e.g. 'Profitability is accelerating while valuation remains attractive...'). Cite specific *current* values vs *historical* values to prove your thesis. Conclude with a verdict on the stock's fundamental setup.>"
}}
""".format(
        date_str=date_str,
        recent_metrics=json.dumps(recent_metrics),
        recent_ratios=json.dumps(recent_ratios)
    )

    try:
        analysis_json = vertex_ai.generate(prompt)
        
        if "{" not in analysis_json:
            raise ValueError("Model output not JSON")

        gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, analysis_json, "application/json")
        gcs.cleanup_old_files(config.GCS_BUCKET_NAME, FUNDAMENTALS_OUTPUT_PREFIX, ticker, analysis_blob_path)
        return analysis_blob_path

    except Exception as e:
        logging.error(f"[{ticker}] Fundamentals analysis failed: {e}")
        return None

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
        # Check if we have the pair AND if we haven't analyzed it yet
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