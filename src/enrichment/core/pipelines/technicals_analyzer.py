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

# --- UPDATED: Example Output with Trade Mechanics ---
_EXAMPLE_OUTPUT = """{
  "score": 0.85,
  "strategy_bias": "Bullish Breakout (Flag Pattern)",
  "analysis": "NVDA is forming a classic 'Bull Flag' consolidation pattern just above its rising 20-day SMA, signaling a continuation of the uptrend. Volume has contracted during the pullback, a healthy sign of supply drying up. The RSI has reset from overbought levels to a constructive 55, leaving room for the next leg up. A daily close above $145 would trigger the breakout, with a measured move target of $160. The thesis is invalidated on a close below the 50-day SMA at $132."
}"""

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_technicals.json'."""
    pattern = re.compile(r"([A-Z.]+)_technicals\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return match.group(1) if match else None

def process_blob(technicals_blob_name: str):
    """Processes one daily technicals file to identify chart patterns and setups."""
    ticker = parse_filename(technicals_blob_name)
    if not ticker:
        return None
    
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_technicals.json"
    logging.info(f"[{ticker}] Generating pattern-based technical analysis")
    
    technicals_content = gcs.read_blob(config.GCS_BUCKET_NAME, technicals_blob_name)
    if not technicals_content:
        logging.warning(f"[{ticker}] No technicals content found for {technicals_blob_name}")
        return None

    price_blob_name = f"{PRICE_INPUT_PREFIX}{ticker}_90_day_prices.json"
    price_content = gcs.read_blob(config.GCS_BUCKET_NAME, price_blob_name)
    
    # If price data is missing, we can't do pattern recognition, so we skip or provide a fallback
    if not price_content:
        logging.warning(f"[{ticker}] No price history found. Cannot perform pattern analysis.")
        return None

    # --- ENHANCED PROMPT: Pattern Recognition & Trade Mechanics ---
    prompt = r"""
You are a master technical analyst and swing trader. Your job is to analyze price action and technical indicators to identify high-probability, short-term trading setups (3-10 day horizon).

Use **only** the JSON data provided.

### 1. Analyze Price Structure (The "Setup")
- Scan the `prices` (OHLCV) array for classic chart patterns: **Bull/Bear Flags, Wedges, Triangles, Head & Shoulders, Double Bottoms/Tops**.
- Identify **Support & Resistance**: Where has price historically pivoted?
- Check for **Volume Confirmation**: Is volume expanding on up-moves and contracting on pullbacks (Bullish)? Or the opposite?

### 2. Analyze Indicators (The "Trigger")
- **Trend:** Is price above key Moving Averages (SMA50, SMA200)? Is the trend accelerating?
- **Momentum:** Check RSI and MACD. Are they confirming the price action or showing **Divergence** (a powerful reversal signal)?
- **Volatility:** Are Bollinger Bands squeezing (indicating an imminent explosive move)?

### 3. Determine the "Conviction Score" (Absolute Scale)
- **0.80 - 1.00 (Strong Buy):** Clear bullish pattern (e.g., Breakout) + Volume Confirmation + Momentum Support.
- **0.60 - 0.79 (Buy):** Bullish trend but lacking a specific explosive catalyst or pattern.
- **0.40 - 0.59 (Neutral):** Choppy, sideways, or conflicting signals. AVOID.
- **0.20 - 0.39 (Sell):** Bearish trend breakdown or resistance rejection.
- **0.00 - 0.19 (Strong Sell):** Clear bearish pattern (e.g., Head & Shoulders break) + Volume Expansion.

### Example Output (Strict Format)
{{example_output}}

### Step-by-Step Execution
1. Identify the dominant chart pattern or trend structure from the `prices` data.
2. Validate the pattern with `technicals` (RSI, MACD, Volume).
3. Define the trade: **What triggers the entry? When is the trade wrong (invalidation)?**
4. Write a dense, trader-focused paragraph. Avoid generic fluff. Use specific numbers.

### Output — return exactly this JSON
{
  "score": <float 0.0 to 1.0>,
  "strategy_bias": "<Short phrase, e.g., 'Bull Flag Breakout', 'Oversold Bounce', 'Bearish Breakdown'>",
  "analysis": "<Dense paragraph (150-250 words). Start with the Pattern/Structure. Mention the Trigger level and the Invalidation level. Cite indicators (RSI, MACD) as confirmation.>"
}

Provided Data:
{
  "technicals": {{technicals_data}},
  "prices": {{price_data}}
}
""".replace("{{technicals_data}}", technicals_content).replace("{{price_data}}", price_content).replace("{{example_output}}", _EXAMPLE_OUTPUT)

    try:
        # REVERTED: Uses default model (Gemini 2.0 Flash)
        analysis_json = vertex_ai.generate(prompt)
        
        if "{" not in analysis_json:
            raise ValueError("Model did not return JSON")
            
        gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, analysis_json, "application/json")
        return analysis_blob_path
        
    except Exception as e:
        logging.error(f"[{ticker}] Failed to generate/save analysis: {e}")
        return None

def run_pipeline():
    logging.info("--- Starting Technicals Pattern Analysis Pipeline ---")
    
    work_items = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
            
    if not work_items:
        logging.info("No new technicals files to process.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
        
    logging.info(f"--- Technicals Analysis Pipeline Finished. Processed {count} files. ---")