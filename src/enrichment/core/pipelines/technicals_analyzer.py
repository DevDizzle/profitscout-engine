# enrichment/core/pipelines/technicals_analyzer.py

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re
import json
from datetime import datetime

INPUT_PREFIX = config.PREFIXES["technicals_analyzer"]["input"]
PRICE_INPUT_PREFIX = "prices/"
OUTPUT_PREFIX = config.PREFIXES["technicals_analyzer"]["output"]

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_technicals.json'."""
    pattern = re.compile(r"([A-Z.]+)_technicals\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return match.group(1) if match else None

def get_latest_data_point(data_list):
    """Safely retrieves the last item in a list."""
    if isinstance(data_list, list) and data_list:
        return data_list[-1]
    return {}

def process_blob(technicals_blob_name: str):
    """Processes one daily technicals file to identify chart patterns and setups."""
    ticker = parse_filename(technicals_blob_name)
    if not ticker:
        return None
    
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_technicals.json"
    logging.info(f"[{ticker}] Generating pattern-based technical analysis")
    
    # 1. Read Technicals (Indicators)
    technicals_content = gcs.read_blob(config.GCS_BUCKET_NAME, technicals_blob_name)
    if not technicals_content:
        return None
    tech_json = json.loads(technicals_content)
    technicals_list = tech_json.get("technicals", [])

    # 2. Read Prices (OHLCV)
    price_blob_name = f"{PRICE_INPUT_PREFIX}{ticker}_90_day_prices.json"
    price_content = gcs.read_blob(config.GCS_BUCKET_NAME, price_blob_name)
    if not price_content:
        logging.warning(f"[{ticker}] No price history found.")
        return None
    price_json = json.loads(price_content)
    price_list = price_json.get("prices", [])

    # 3. CRITICAL FIX: Extract the LATEST snapshot in Python
    # We strip the lists down to just the last 60 days to reduce noise,
    # but we explicitly pull the very last row for the "Current Status".
    
    latest_tech = get_latest_data_point(technicals_list)
    latest_price = get_latest_data_point(price_list)
    
    current_date = latest_price.get("date", "Unknown")
    
    # Slice for context (last 45 days is enough for pattern rec)
    recent_prices = price_list[-45:]
    recent_techs = technicals_list[-45:]

    # --- ENHANCED PROMPT: Explicit Current State Anchor ---
    prompt = r"""
You are a master technical analyst. Analyze the provided data to identify the CURRENT trading setup for {ticker} as of {current_date}.

### DATA HIERARCHY (CRITICAL)
1. **CURRENT SNAPSHOT**: This is the ABSOLUTE TRUTH for price and indicators right now. You must NOT cite data older than this date as "current".
2. **Recent History**: Use this only to determine the trend (e.g., is the stock rising into this price, or falling into it?).

### Current Snapshot ({current_date})
- **Price:** {latest_price}
- **Indicators:** {latest_tech}

### Task
1.  **Trend Identification**: Look at the `Recent History`. Is the stock making higher highs (Uptrend) or lower lows (Downtrend)?
2.  **Current Status**: Look at the `Current Snapshot`. Is RSI overbought (>70) or oversold (<30) *TODAY*? Where is the price relative to the SMA_50 *TODAY*?
3.  **Pattern Recognition**: Identify the pattern forming over the last 10-20 data points (e.g., Bull Flag, Consolidation, Parabolic Extension).

### Scoring Rules
- **0.80 - 1.00 (Bullish Breakout):** Price is trending up AND consolidating near highs OR breaking out on volume.
- **0.60 - 0.79 (Bullish Trend):** Above SMA50, steady uptrend, no immediate breakout signal.
- **0.40 - 0.59 (Neutral/Choppy):** Stuck in a range, or conflicting signals (e.g., price up but RSI divergent).
- **0.20 - 0.39 (Bearish Trend):** Below SMA50, making lower lows.
- **0.00 - 0.19 (Bearish Breakdown):** Breaking support on volume.

### Output Requirements
Return strictly valid JSON.
- **score**: Float (0.0 to 1.0).
- **strategy_bias**: Short phrase (e.g., "Bull Flag", "Trend Continuation", "Overextended - Wait").
- **analysis**: A dense paragraph. **You MUST reference the price and RSI from the 'Current Snapshot' block.** Do not reference data from a month ago as 'current'.

{{
  "score": <float>,
  "strategy_bias": "<string>",
  "analysis": "<string>"
}}

### Historical Context (Last 45 Days)
Prices: {recent_prices}
Indicators: {recent_techs}
""".format(
        ticker=ticker,
        current_date=current_date,
        latest_price=json.dumps(latest_price),
        latest_tech=json.dumps(latest_tech),
        recent_prices=json.dumps(recent_prices),
        recent_techs=json.dumps(recent_techs)
    )

    try:
        # Use default model (Gemini 2.0 Flash)
        analysis_json = vertex_ai.generate(prompt)
        
        # Clean markdown if present
        analysis_json = analysis_json.replace("```json", "").replace("```", "").strip()
        
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