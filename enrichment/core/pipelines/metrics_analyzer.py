import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re

INPUT_PREFIX = config.PREFIXES["metrics_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["metrics_analyzer"]["output"]

# One-shot example for consistent output format (format anchor only)
_EXAMPLE_OUTPUT = """{
  "score": 0.54,
  "analysis": "Adobe's financial performance over the last eight quarters presents a mixed picture, leaning slightly bullish. Revenue per share has generally trended upwards, with recent quarters showing consistent growth, indicating strong demand for its products and services. Net income per share, however, has fluctuated, suggesting some volatility in profitability despite revenue gains. Operating and free cash flow per share have also experienced fluctuations, but recent data indicates a healthy generation of cash. Valuation multiples such as PE, price-to-sales, and price-to-book ratios are relatively high, reflecting investor expectations for future growth, but also potentially indicating that the stock is richly valued. However, these multiples have generally decreased over the past few quarters, which is a bullish signal if fundamentals are improving. Profitability metrics like ROE and ROIC have shown some volatility, but recent figures suggest stabilization. The company maintains a relatively low debt-to-equity ratio and a manageable debt-to-assets ratio, suggesting a conservative capital structure. Liquidity, as measured by the current ratio, is generally stable, hovering around 1.0, indicating the company's ability to meet its short-term obligations. While the interest coverage ratio is strong, it has fluctuated significantly. Overall, Adobe demonstrates solid revenue growth and cash generation capabilities, but its high valuation multiples warrant caution. The risk-reward profile appears moderately bullish, contingent on the company's ability to sustain its revenue growth and improve profitability in the coming quarters."
}"""

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_2025-06-30.json'."""
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)

def process_blob(blob_name: str):
    """Processes one key metrics file."""
    ticker, date_str = parse_filename(blob_name)
    if not ticker or not date_str:
        return None
    
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_{date_str}.json"
    logging.info(f"[{ticker}] Generating key metrics analysis for {date_str}")
    
    content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    if not content:
        return None
    
    prompt = r"""You are a seasoned equity valuation analyst evaluating a company’s **key metrics** over the last eight quarters to assess attractiveness for the next 6-12 months.
Use **only** the JSON provided — do **not** use external data or assumptions.

### Key Interpretation Guidelines
1. **Valuation Multiples** - Lower multiples with improving fundamentals are bullish; high/expanding multiples without support are bearish.
2. **Profitability & Returns** - Rising ROE/ROIC, improving margins, and higher cash flow per share are bullish.
3. **Leverage & Solvency** - Lower debt ratios and rising coverage are bullish; rising leverage is bearish.
4. **Liquidity & Efficiency** - Higher current ratios, efficient working capital are bullish; deterioration is bearish.
5. **Growth Signals** - Revenue/share growth and expanding yields are bullish; declines are bearish.
6. **No Material Signals** - If flat and balanced, output 0.50 and state valuation appears neutral.

### Example Output (for format only; do not copy values or wording)
EXAMPLE_OUTPUT:
{{example_output}}

### Step-by-Step Reasoning
1. Compute quarter-over-quarter and year-over-year changes for each metric.
2. Classify each as bullish, bearish, or neutral.
3. Map the net to probability bands:
   - 0.00-0.30 - clearly bearish
   - 0.31-0.49 - mildly bearish
   - 0.50       - neutral / balanced
   - 0.51-0.69 - moderately bullish
   - 0.70-1.00 - strongly bullish
4. Summarize the decisive metrics in one dense paragraph.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (200-400 words) summarizing valuation context, profitability trends, leverage & liquidity considerations, and a risk-reward verdict>"
}

Provided data:
{{key_metrics_data}}
""".replace("{{key_metrics_data}}", content).replace("{{example_output}}", _EXAMPLE_OUTPUT)

    analysis_json = vertex_ai.generate(prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, analysis_json, "application/json")
    gcs.cleanup_old_files(config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, analysis_blob_path)
    return analysis_blob_path

def run_pipeline():
    logging.info("--- Starting Key Metrics Analysis Pipeline ---")
    all_inputs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    all_analyses = set(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX))
    
    work_items = [
        blob for blob in all_inputs 
        if not f"{OUTPUT_PREFIX}{os.path.basename(blob)}" in all_analyses
    ]
            
    if not work_items:
        logging.info("All key metrics analyses are up-to-date.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- Key Metrics Analysis Pipeline Finished. Processed {count} new files. ---")
