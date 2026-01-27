# enrichment/core/pipelines/financials_analyzer.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re
import json

INPUT_PREFIX = config.PREFIXES["financials_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["financials_analyzer"]["output"]

# --- ALIAS MAPPING: Defense against API inconsistency ---
# FMP usually normalizes this, but we check aliases just in case.
METRIC_ALIASES = {
    "revenue": ["revenue", "sales", "totalRevenue", "totalSales"],
    "grossProfitRatio": ["grossProfitRatio", "grossMargin"],
    "operatingIncome": ["operatingIncome", "operatingProfit"],
    "netIncome": ["netIncome", "netProfit", "netEarnings"],
    "cashAndEquivalents": ["cashAndCashEquivalents", "cash", "totalCash"],
    "totalDebt": ["totalDebt", "shortLongTermDebtTotal"],
    "netDebt": ["netDebt"],
    "operatingCashFlow": ["operatingCashFlow", "netCashProvidedByOperatingActivities"],
    "freeCashFlow": ["freeCashFlow"],
    "capitalExpenditure": ["capitalExpenditure", "investmentsInPropertyPlantAndEquipment"]
}

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_2025-06-30.json'."""
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)

def _smart_get(data: dict, metric_name: str):
    """
    Tries to find a metric using a list of known aliases.
    Returns the first non-None value found, or None.
    """
    aliases = METRIC_ALIASES.get(metric_name, [metric_name])
    for alias in aliases:
        val = data.get(alias)
        if val is not None:
            return val
    return None

def _extract_financial_trends(json_content: str, periods: int = 5) -> list:
    """
    Extracts high-signal metrics using defensive alias checking.
    """
    if not json_content:
        return []
    
    try:
        data = json.loads(json_content)
        reports = data.get("quarterly_reports", [])
        if not reports:
            return []
            
        # Sort by date descending (newest first) and take top N
        sorted_reports = sorted(reports, key=lambda x: x.get("date", ""), reverse=True)
        recent_reports = sorted_reports[:periods]
        
        simplified_data = []
        for report in recent_reports:
            date = report.get("date")
            inc = report.get("income_statement", {})
            bal = report.get("balance_sheet", {})
            cf = report.get("cash_flow_statement", {})
            
            record = {
                "date": date,
                # Income
                "revenue": _smart_get(inc, "revenue"),
                "grossProfitRatio": _smart_get(inc, "grossProfitRatio"),
                "operatingIncome": _smart_get(inc, "operatingIncome"),
                "netIncome": _smart_get(inc, "netIncome"),
                # Balance Sheet
                "cashAndEquivalents": _smart_get(bal, "cashAndEquivalents"),
                "totalDebt": _smart_get(bal, "totalDebt"),
                "netDebt": _smart_get(bal, "netDebt"),
                # Cash Flow
                "operatingCashFlow": _smart_get(cf, "operatingCashFlow"),
                "freeCashFlow": _smart_get(cf, "freeCashFlow"),
                "capitalExpenditure": _smart_get(cf, "capitalExpenditure")
            }
            simplified_data.append(record)
            
        return simplified_data

    except (json.JSONDecodeError, AttributeError):
        return []

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
    
    # --- STEP 1: Smart Extraction ---
    financial_trends = _extract_financial_trends(content, periods=5)
    
    if not financial_trends:
        logging.warning(f"[{ticker}] No valid financial records found after extraction.")
        return None

    # --- STEP 2: The "Virtual Agents" Prompt ---
    prompt = r"""
You are a forensic financial analyst. Your task is to evaluate the provided quarterly financial data to determine the company's operational health and direction.

### ANALYSIS DATE: {date_str}
Treat the record with date `{date_str}` as **CURRENT**. The other records are historical context for identifying trends (Year-Over-Year or Quarter-Over-Quarter).

### Curated Financial Data (Last 5 Quarters)
{financial_trends}

### Analysis Tasks
1.  **Income Statement Analysis**: Is Revenue growing or shrinking? Are Margins (`grossProfitRatio`) expanding or compressing?
2.  **Cash Flow Analysis**: Check `operatingCashFlow` and `freeCashFlow`. Is the company actually generating cash from operations, or is it burning cash?
3.  **Balance Sheet Analysis**: Compare `totalDebt` to `cashAndEquivalents`. Is leverage increasing to dangerous levels?

### Formatting Rules
- **ALWAYS** abbreviate large numbers using "B" for Billions and "M" for Millions.
- Examples: Write `$1.5B` instead of `$1,500,000,000`. Write `$200M` instead of `$200,000,000`.
- Use 1-2 decimal places where appropriate (e.g., `$1.25B`).

### Scoring Guide
- **0.0 - 0.3 (Bearish):** Cash burn, declining revenue, rising debt, or negative margins.
- **0.4 - 0.6 (Neutral):** Stable but stagnant, or mixed signals (e.g., profitable but declining revenue).
- **0.7 - 1.0 (Bullish):** Accelerating revenue, expanding margins, positive and growing Free Cash Flow.

### Output — return exactly this JSON
{{
  "score": <float between 0.0 and 1.0>,
  "analysis": "<One dense paragraph (150-250 words). Synthesize the findings from the three tasks above. You MUST cite specific numbers using the 'B' and 'M' abbreviations (e.g., 'Revenue grew to $8.4B...', 'FCF turned negative at -$50M...') to support your verdict.>"
}}
""".format(
        date_str=date_str,
        financial_trends=json.dumps(financial_trends, indent=2)
    )

    try:
        analysis_json = vertex_ai.generate(prompt)
        
        # Basic validation
        if "{" not in analysis_json:
            raise ValueError("Model output not JSON")

        gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, analysis_json, "application/json")
        gcs.cleanup_old_files(config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, analysis_blob_path)
        return analysis_blob_path

    except Exception as e:
        logging.error(f"[{ticker}] Financials analysis failed: {e}")
        return None

def run_pipeline():
    """
    Finds and processes financial statement files.
    Implements timestamp-based caching: Only re-runs analysis if the input data 
    is newer than the existing analysis output.
    """
    logging.info("--- Starting Financials Analysis Pipeline ---")
    
    # Fetch all files with metadata (updated timestamps)
    all_input_blobs = gcs.list_blobs_with_properties(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    all_analysis_blobs = gcs.list_blobs_with_properties(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX)
    
    # Map filenames to timestamps for easier lookup
    # Note: Input is like 'merged_financials/TICKER_DATE.json', output is 'financials_analysis/TICKER_DATE.json'
    # We match on basename.
    inputs_map = {os.path.basename(k): (k, v) for k, v in all_input_blobs.items()}
    analysis_map = {os.path.basename(k): v for k, v in all_analysis_blobs.items()}
    
    work_items = []
    skipped_count = 0
    
    for file_name, (full_blob_path, input_timestamp) in inputs_map.items():
        # Check if we already have an analysis for this file
        if file_name in analysis_map:
            analysis_timestamp = analysis_map[file_name]
            
            # CACHE LOGIC: If Analysis is NEWER than Input, we can skip.
            if analysis_timestamp > input_timestamp:
                skipped_count += 1
                continue
            else:
                ticker, _ = parse_filename(file_name)
                logging.info(f"[{ticker}] Financials updated (Input: {input_timestamp} > Analysis: {analysis_timestamp}). Re-running.")
        
        # If we are here, we need to process (either missing or outdated)
        work_items.append(full_blob_path)
            
    if not work_items:
        logging.info(f"All financials analyses are up-to-date. (Skipped {skipped_count})")
        return

    logging.info(f"Found {len(work_items)} financial files to analyze (Skipped {skipped_count} up-to-date).")

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- Financials Analysis Pipeline Finished. Processed {count} new files. ---")