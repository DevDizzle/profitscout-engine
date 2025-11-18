# enrichment/core/pipelines/technicals_analyzer.py

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

# --- NEW: BEARISH Example Output for a Premium BUYER (PUTS) ---
_EXAMPLE_OUTPUT = """{
  "score": 0.15,
  "strategy_bias": "Strong Bearish Breakdown",
  "analysis": "AAON is exhibiting clear signs of a bearish breakdown, making it a strong candidate for buying put options. The price has decisively breached its critical 200-day SMA on high volume, confirming a major trend reversal. The MACD has posted a bearish cross and is accelerating downward (-1.20), while the RSI has plunged to 28, indicating strong selling pressure and oversold conditions that can persist in a strong downtrend. This technical alignment suggests the path of least resistance is lower, with high potential for a sharp, volatile move down as long-term support has failed. The setup favors directional short strategies that profit from both a price decline and an expansion in volatility."
}"""

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_technicals.json'."""
    pattern = re.compile(r"([A-Z.]+)_technicals\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return match.group(1) if match else None

def process_blob(technicals_blob_name: str):
    """Processes one daily technicals file to identify breakout potential."""
    ticker = parse_filename(technicals_blob_name)
    if not ticker:
        return None
    
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_technicals.json"
    logging.info(f"[{ticker}] Generating technicals analysis for premium buying")
    
    technicals_content = gcs.read_blob(config.GCS_BUCKET_NAME, technicals_blob_name)
    if not technicals_content:
        logging.warning(f"[{ticker}] No technicals content found for {technicals_blob_name}")
        return None

    price_blob_name = f"{PRICE_INPUT_PREFIX}{ticker}_90_day_prices.json"
    price_content = gcs.read_blob(config.GCS_BUCKET_NAME, price_blob_name)
    price_data_for_prompt = price_content if price_content else '"prices": []'

    # --- Prompt is already correct, just using a new bearish example ---
    prompt = r"""You are a quantitative technical analyst identifying short-term breakout opportunities for an options premium BUYER. Your goal is to find setups with strong directional momentum and high potential for a volatile price move over the next 5-30 trading days.

Use **only** the JSON data provided.

### Key Interpretation Guidelines
1.  **Trend & Momentum (Primary):** Is there a clear, accelerating trend? Is price decisively above/below key moving averages (e.g., 21-day EMA, 50-day SMA)? Is the MACD showing a strong cross and pulling away from its signal line? Is the RSI in a strong trend (ideally >60 for bullish, <40 for bearish)?
2.  **Breakout Potential:** Is the price breaking out of a recent consolidation pattern, support, or resistance? A stock moving from a tight range to a new trend is an ideal setup.
3.  **Synthesize for a Breakout:** Combine these factors into a single thesis. A weak or sideways trend is undesirable. Your goal is to find conviction. A score near 0.5 indicates a poor setup for a premium buyer.

### Example Output (for format only; do not copy wording)
{{example_output}}

### Step-by-Step Reasoning
1.  Evaluate the strength and direction of the trend and momentum indicators (MACD, RSI).
2.  Assess if the price action confirms a breakout or a breakdown.
3.  Determine if the stock is more likely to be directional (good for buying premium) or range-bound (bad).
4.  Map the net directional strength to a probability score. High conviction breakouts get scores far from 0.5.
    -   0.80-1.00 → Strong Bullish Breakout (High conviction to buy calls)
    -   0.60-0.79 → Developing Bullish Momentum (Favorable for buying calls)
    -   0.40-0.59 → Neutral / No Clear Edge (AVOID - high risk of premium decay)
    -   0.20-0.39 → Developing Bearish Momentum (Favorable for buying puts)
    -   0.00-0.19 → Strong Bearish Breakdown (High conviction to buy puts)
5.  Select a strategy bias and write a dense, evidence-based analysis.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1 indicating directional conviction>,
  "strategy_bias": "<Categorical bias: 'Strong Bullish Breakout', 'Developing Bullish Momentum', 'Neutral / No Clear Edge', 'Developing Bearish Momentum', 'Strong Bearish Breakdown'>",
  "analysis": "<One dense paragraph (150-250 words) summarizing the key technical signals that support a directional breakout or breakdown, and why it's a suitable candidate for buying premium.>"
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
    logging.info("--- Starting Technicals Breakout Analysis Pipeline (for Premium Buyers) ---")
    work_items = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
            
    if not work_items:
        logging.info("No new technicals files to process.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- Technicals Analysis Pipeline Finished. Processed {count} files. ---")