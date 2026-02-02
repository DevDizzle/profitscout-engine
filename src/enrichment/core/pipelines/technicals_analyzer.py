# enrichment/core/pipelines/technicals_analyzer.py

import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from .. import config, gcs
from ..clients import vertex_ai

INPUT_PREFIX = config.PREFIXES["technicals_analyzer"]["input"]
PRICE_INPUT_PREFIX = "prices/"
OUTPUT_PREFIX = config.PREFIXES["technicals_analyzer"]["output"]

# --- CONFIG: Reduce Noise for LLM ---
HISTORY_WINDOW_DAYS = 30
KEEP_INDICATORS = {
    "date",
    "RSI_14",
    "MACD_12_26_9",
    "MACDh_12_26_9",  # MACD Line and Histogram
    "SMA_50",
    "SMA_200",
    "EMA_21",
    "OBV",  # On-Balance Volume for confirming breakouts
}


# --- RATE LIMITER (Throttled Concurrency) ---
class RateLimiter:
    """
    Thread-safe rate limiter to ensure we don't exceed Vertex AI quotas.
    Target: 50 RPM (1 request every ~1.2 seconds).
    """

    def __init__(self, interval=1.2):
        self.interval = interval
        self.last_call = 0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            wait_time = self.interval - elapsed
            if wait_time > 0:
                time.sleep(wait_time)
            self.last_call = time.time()


# Initialize global limiter (Shared across threads)
_limiter = RateLimiter(interval=1.2)


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


def _filter_indicators(tech_list: list[dict]) -> list[dict]:
    """
    Strips out noisy columns (e.g. BBL, ADX, STOCH) to focus the LLM
    on Price + Core Momentum/Trend.
    """
    clean_list = []
    for row in tech_list:
        clean_row = {k: v for k, v in row.items() if k in KEEP_INDICATORS}
        # Ensure date is always present if it wasn't in KEEP_INDICATORS
        if "date" in row:
            clean_row["date"] = row["date"]
        clean_list.append(clean_row)
    return clean_list


def process_blob(technicals_blob_name: str):
    """Processes one daily technicals file to identify chart patterns and setups."""
    # WRAP ENTIRE LOGIC IN TRY/EXCEPT
    try:
        ticker = parse_filename(technicals_blob_name)
        if not ticker:
            return None

        # Standard filename (No date in name, to preserve downstream compatibility)
        analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_technicals.json"

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

        # --- CRITICAL FIX: Synchronize Sort Order (Oldest -> Newest) ---
        # FMP prices come Descending (Newest first). We MUST sort Ascending.
        try:
            price_list.sort(key=lambda x: x.get("date", ""))
            technicals_list.sort(key=lambda x: x.get("date", ""))
        except Exception as e:
            logging.error(f"[{ticker}] Critical sorting error: {e}", exc_info=True)
            return None

        # 3. Extract the TRUE Latest Snapshot (Post-Sort)
        latest_tech = get_latest_data_point(technicals_list)
        latest_price = get_latest_data_point(price_list)

        current_date = latest_price.get("date", "Unknown")

        # 4. Prune Data: Last 30 Days + Filtered Indicators
        # Reduces token count and forces model to look at short-term structure.
        recent_prices = price_list[-HISTORY_WINDOW_DAYS:]

        raw_recent_techs = technicals_list[-HISTORY_WINDOW_DAYS:]
        recent_techs = _filter_indicators(raw_recent_techs)

        # --- GUARD: Skip if insufficient data ---
        if not recent_prices or len(recent_prices) < 10 or not recent_techs:
            logging.warning(
                f"[{ticker}] Insufficient data (Prices: {len(recent_prices)}, Techs: {len(recent_techs)}). Skipping."
            )
            return None

        # --- ENHANCED PROMPT: Explicit Current State Anchor ---
        prompt = r"""
You are a master technical analyst. Analyze the provided data to identify the CURRENT trading setup for {ticker} as of {current_date}.

### DATA HIERARCHY (CRITICAL)
1. **CURRENT SNAPSHOT**: This is the ABSOLUTE TRUTH for price and indicators right now. You must NOT cite data older than this date as "current".
2. **Recent History (30 Days)**: Use this only to identify the Formation/Pattern (e.g., Bull Flag, Double Bottom, Channel).

### Current Snapshot ({current_date})
- **Price:** {latest_price}
- **Indicators:** {latest_tech}

### Task
1.  **Trend Identification**: Look at the `Recent History`. Is the stock making higher highs (Uptrend) or lower lows (Downtrend)?
2.  **Current Status**: Look at the `Current Snapshot`. Is RSI overbought (>70) or oversold (<30) *TODAY*? Where is the price relative to the SMA_50 *TODAY*?
3.  **Pattern Recognition**: Identify the pattern forming over the last 15-30 days (e.g., Bull Flag, Consolidation, Parabolic Extension).

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

### Historical Context (Last {window} Days)
Prices: {recent_prices}
Indicators: {recent_techs}
""".format(
            ticker=ticker,
            current_date=current_date,
            window=HISTORY_WINDOW_DAYS,
            latest_price=json.dumps(latest_price),
            # Filter the latest snapshot too for consistency
            latest_tech=json.dumps(
                {k: v for k, v in latest_tech.items() if k in KEEP_INDICATORS}
            ),
            recent_prices=json.dumps(recent_prices),
            recent_techs=json.dumps(recent_techs),
        )

        # --- RATE LIMITER: Enforce 1 call every 1.2s across all threads ---
        _limiter.wait()

        # Use default model (Gemini 2.0 Flash)
        # Note: vertex_ai.generate might return text with markdown formatting
        # FAIL FAST: Timeout handled by client init
        response_text = vertex_ai.generate(
            prompt, response_mime_type="application/json"
        )

        # Robust JSON Extraction
        json_str = response_text.strip()

        # 1. Try extracting from markdown code blocks
        json_match = re.search(r"```json\s*(.*?)\s*```", response_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            # 2. Try generic code block
            code_match = re.search(r"```\s*(.*?)\s*```", response_text, re.DOTALL)
            if code_match:
                json_str = code_match.group(1)
            else:
                # 3. Fallback: Find outermost braces
                start_idx = response_text.find("{")
                end_idx = response_text.rfind("}")
                if start_idx != -1 and end_idx != -1:
                    json_str = response_text[start_idx : end_idx + 1]

        # Validate by parsing
        json.loads(json_str)

        gcs.write_text(
            config.GCS_BUCKET_NAME, analysis_blob_path, json_str, "application/json"
        )
        return analysis_blob_path

    except json.JSONDecodeError as je:
        logging.error(f"[{ticker}] Invalid JSON from model: {je}")
        return None
    except Exception as e:
        logging.error(f"[{ticker}] Failed to generate/save analysis: {e}")
        return None


def run_pipeline():
    logging.info(
        "--- Starting Technicals Pattern Analysis Pipeline (Parallel + Throttled) ---"
    )

    # 1. DELETE ALL FILES UP FRONT (Ensure 1 file per ticker, fresh run)
    try:
        logging.info(f"Deleting all files in output prefix: {OUTPUT_PREFIX}")
        gcs.delete_all_in_prefix(config.GCS_BUCKET_NAME, OUTPUT_PREFIX)
    except Exception as e:
        logging.error(f"Failed to clean up output prefix: {e}")
        # Proceeding anyway as we will overwrite

    # 2. Get List of Inputs (Materialize List to Fail Fast)
    logging.info("Listing inputs...")
    try:
        work_items = list(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX))
    except Exception as e:
        logging.error(f"Failed to list blobs: {e}")
        return

    if not work_items:
        logging.info("No new technicals files to process.")
        return

    total_files = len(work_items)
    logging.info(
        f"Processing {total_files} technicals files with {config.MAX_WORKERS} workers..."
    )

    # 3. Process with ThreadPool (Manual management to skip "wait=True")
    processed_count = 0
    executor = ThreadPoolExecutor(max_workers=config.MAX_WORKERS)
    try:
        futures = {executor.submit(process_blob, item): item for item in work_items}

        for i, future in enumerate(as_completed(futures)):
            try:
                if future.result():
                    processed_count += 1
            except Exception as e:
                logging.error(f"Thread failed: {e}")

            # Progress Logging
            if (i + 1) % 50 == 0:
                logging.info(f"Progress: {i + 1}/{total_files} files processed...")
    finally:
        # CRITICAL: Do not wait for zombie threads (e.g. stuck socket close)
        # Force shutdown so the Cloud Function returns '200 OK' immediately.
        logging.info("Forcing executor shutdown (wait=False)...")
        executor.shutdown(wait=False, cancel_futures=True)

    logging.info(
        f"--- Technicals Analysis Pipeline Finished. Processed {processed_count}/{total_files} files. ---"
    )
