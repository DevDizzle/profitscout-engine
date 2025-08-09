import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re
from datetime import datetime
import json

INPUT_PREFIX = config.PREFIXES["mda_summarizer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["mda_summarizer"]["output"]

def parse_filename(blob_name: str):
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    if not match:
        return None, None, None, None, None
    ticker, date_str = match.groups()
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        quarter = (date_obj.month - 1) // 3 + 1
        year = date_obj.year
        filing_type = "10-Q" if date_obj.month in [3, 6, 9] else "10-K"
        return ticker, date_str, filing_type, year, quarter
    except ValueError:
        return None, None, None, None, None

def read_mda_data(raw_json: str):
    try:
        return json.loads(raw_json).get("mda")
    except (json.JSONDecodeError, TypeError):
        return None

def process_blob(blob_name: str):
    ticker, date_str, filing_type, year, quarter = parse_filename(blob_name)
    if not all([ticker, date_str, filing_type, year, quarter]):
        return None
    
    summary_blob_path = f"{OUTPUT_PREFIX}{ticker}_{date_str}.txt"
    logging.info(f"[{ticker}] Generating MD&A summary for {date_str}")
    
    raw_json = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    mda_content = read_mda_data(raw_json)
    if not mda_content:
        return None

    mda_prompt = r"""You are a financial analyst summarizing the MD&A (Management's Discussion & Analysis) for {ticker} (Q{quarter} {year}) from a {filing_type} filing.
Use step-by-step reasoning internally to extract the drivers of performance, liquidity, risks, and forward signals that matter for short-term stock movement (30-90 days).
Produce ONE dense, coherent narrative optimized for FinBERT embeddings and NLP tasks (e.g., LDA). Generalize across all Russell 1000 sectors.

Pre-processing rules:
- Decode HTML entities (e.g., &#8217; -> ', &nbsp; -> space).
- Use only the provided MD&A text; do not invent facts or pull outside information.
- If a data point is missing, state this briefly (e.g., "No guidance provided.").

Reasoning Steps (use privately; do not output as bullets):
1) Results of Operations & Drivers
   - Quantify YoY/QoQ changes in revenue, margins (gross/operating), EPS (if present), and segment/geography contributions.
   - Attribute movements to concrete drivers (pricing, mix, volumes/traffic, utilization, FX, cost inflation/deflation, supply chain, one-offs).
   - Distinguish non-recurring items from run-rate impacts.

2) Non-GAAP & Adjustments
   - Extract non-GAAP metrics (e.g., adjusted EPS, EBITDA, FCF, CASM ex-fuel, PRASM, ARR/NRR, same-store sales).
   - Summarize reconciliation highlights and indicate whether adjustments appear recurring (restructuring, litigation, SBC, acquisition-related) or one-time.

3) Liquidity & Capital Resources
   - Report liquidity (cash, short-term investments, undrawn facilities), cash flows (CFO/CFI/CFF), and key drivers.
   - Note capital allocation and balance sheet actions: capex, buybacks/dividends, debt issuance/repayment, leverage trends, maturity walls, covenant considerations.
   - Include any ratings, collateral, LTV, or minimum-liquidity covenants mentioned.

4) Costs & Efficiency
   - Identify cost line trends (COGS, opex, wage/benefit inflation, selling expense, fuel/energy/commodities) and productivity/efficiency programs.
   - Call out unit economics/KPIs when present (e.g., CASM/PRASM/TRASM for airlines; opex per unit, utilization, yields, backlog turns, GM% for hardware; NIM/CET1 for banks; ARR/GRR/NRR/Gross Margin for SaaS; same-store sales for retail; realized prices/production for energy).

5) Risk Factors & Exogenous Events
   - Extract macro/industry risks (rates, FX, tariffs, regulatory, litigation, safety incidents, recalls), supply chain dependencies, hedging posture, and sensitivity commentary (e.g., +/- $X per 1% move in rates/fuel).

6) Outlook & Guidance
   - Capture explicit guidance (ranges, raised/lowered/affirmed), qualitative outlook, demand commentary, and management tone (confident/cautious/mixed).
   - Include leading indicators (bookings, backlog, pipeline, traffic, PRASM, order intake, churn, net adds).

7) Signal Synthesis for Near-Term Price Impact (30-90 days)
   - Weave the above into a concise assessment of likely direction and volatility catalysts (e.g., lowered guide + margin compression = negative skew; cost actions + debt paydown = supportive).
   - Prefer concrete, MD&A-grounded phrases (e.g., "expects lower domestic demand", "committed to $X capex", "covenant headroom of ...").

Final Output Instructions:
- Produce one continuous, dense narrative (approximately 800-1,000 words) in full sentences and paragraphs. No section headers or bullet lists in the body.
- Integrate specific figures/percentages where given; avoid tables.
- End with exactly one line:
  Topic keywords: keyword1, keyword2, ...
  (Provide 20-30 comma-separated keywords/phrases capturing metrics, drivers, risks, guidance, and catalysts. No quotes.)

Input MD&A text:
{mda}
""".format(
        ticker=ticker,
        quarter=quarter,
        year=year,
        filing_type=filing_type,
        mda=mda_content,
    )

    summary_text = vertex_ai.generate(mda_prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, summary_blob_path, summary_text)
    gcs.cleanup_old_files(config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, summary_blob_path)
    return summary_blob_path

def run_pipeline():
    logging.info("--- Starting MD&A Summarizer Pipeline ---")
    all_mda_blobs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    all_summaries = set(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX))
    
    work_items = []
    for blob_name in all_mda_blobs:
        if not blob_name.endswith(".json"):
            continue
        ticker, date_str, _, _, _ = parse_filename(blob_name)
        if not ticker or not date_str:
            continue
        if f"{OUTPUT_PREFIX}{ticker}_{date_str}.txt" not in all_summaries:
            work_items.append(blob_name)
            
    if not work_items:
        logging.info("All MD&A summaries are up-to-date.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- MD&A Summarizer Pipeline Finished. Processed {count} new files. ---")
