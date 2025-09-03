import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re
import json

INPUT_PREFIX = config.PREFIXES["technicals_analyzer"]["input"]
PRICE_INPUT_PREFIX = "prices/"
OUTPUT_PREFIX = config.PREFIXES["technicals_analyzer"]["output"]

# --- MODIFIED: A more concise one-shot example ---
_EXAMPLE_OUTPUT = """{
  "score": 0.35,
  "analysis": "Amazon's recent technicals suggest a mildly bearish trend. The price has fallen below its 21-day EMA, which is now acting as resistance, and momentum indicators like the MACD and RSI are showing signs of weakening. The negative MACD histogram and an RSI below 50 both signal reduced buying pressure. While the price remains above the longer-term 50-day and 200-day SMAs, the recent pattern of lower highs and lower lows suggests these levels could be tested soon. The overall picture points to a short-term downtrend with potential for further declines if key support levels are breached."
}"""

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_technicals.json'."""
    pattern = re.compile(r"([A-Z.]+)_technicals\\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return match.group(1) if match else None

def process_blob(technicals_blob_name: str):
    """Processes one daily technicals file and its corresponding price data."""
    ticker = parse_filename(technicals_blob_name)
    if not ticker:
        return None
    
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_technicals.json"
    logging.info(f"[{ticker}] Generating technicals analysis")
    
    technicals_content = gcs.read_blob(config.GCS_BUCKET_NAME, technicals_blob_name)
    if not technicals_content:
        logging.warning(f"[{ticker}] No technicals content found for {technicals_blob_name}")
        return None

    price_blob_name = f"{PRICE_INPUT_PREFIX}{ticker}_90_day_prices.json"
    price_content = gcs.read_blob(config.GCS_BUCKET_NAME, price_blob_name)
    price_data_for_prompt = price_content if price_content else '"prices": []'


    # --- MODIFIED: Updated prompt for a shorter, more direct analysis ---
    prompt = r"""You are a sharp technical analyst writing for a fast-paced audience. Evaluate the provided technical indicators and 90-day price history to assess the likely direction over the next 1–3 months.
Use **only** the JSON data provided.

### Key Interpretation Guidelines
1.  **Price Action**: What is the recent trend? Is it breaking out or breaking down?
2.  **Moving Averages**: Is the price above or below key moving averages (e.g., 21-day EMA, 50/200-day SMA)?
3.  **Momentum**: Are indicators like MACD and RSI showing strength or weakness?
4.  **No Material Signals**: If mixed or neutral, output 0.50.

### Example Output (for format only; do not copy wording)
{{example_output}}

### Step-by-Step Reasoning
1.  Evaluate changes in price, trend, and momentum.
2.  Map the net result to probability bands:
    -   0.00-0.30 → clearly bearish
    -   0.31-0.49 → mildly bearish
    -   0.50       → neutral / balanced
    -   0.51-0.69 → moderately bullish
    -   0.70-1.00 → strongly bullish
3.  Summarize key signals into one dense paragraph.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (150-200 words) summarizing key trends, patterns, and technical reasoning.>"
}

Provided data:
{
  "technicals": {{technicals_data}},
  "prices": {{price_data}}
}
""".replace("{{technicals_data}}", technicals_content).replace("{{price_data}}", price_data_for_prompt).replace("{{example_output}}", _EXAMPLE_OUTPUT)

    analysis_json = vertex_ai.generate(prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, analysis_json, "application/json")
    return analysis_blob_path

def run_pipeline():
    logging.info("--- Starting Technicals Analysis Pipeline (Daily Run) ---")
    work_items = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
            
    if not work_items:
        logging.info("No new technicals files to process.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- Technicals Analysis Pipeline Finished. Processed {count} files. ---")