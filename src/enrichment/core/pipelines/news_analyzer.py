# enrichment/core/pipelines/news_analyzer.py

import datetime
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage

from .. import config, gcs
from ..clients import vertex_ai

INPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["output"]


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


# Initialize global limiter
_limiter = RateLimiter(interval=1.2)

# Keeps your existing output format
_EXAMPLE_OUTPUT = """{
  "score": 0.85,
  "catalyst_type": "Strong Bullish Catalyst",
  "analysis": "The latest earnings report confirms a significant acceleration in AI data center revenue, which beat expectations by 15%. Management raised full-year guidance, citing 'unprecedented demand' for the new chip architecture. While macro headwinds persist in the consumer segment, this specific enterprise catalyst is strong enough to drive a breakout."
}"""


def parse_filename(blob_name: str):
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    if match:
        return (match.group(1), match.group(2))
    return (None, None)


def _extract_json_object(text: str) -> str:
    if not text:
        return ""
    # Strip code fences
    text = re.sub(r"^\s*```json\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"```\s*$", "", text, flags=re.MULTILINE)
    text = text.strip()
    # Find the JSON bracket boundaries
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]
    return text


def _format_news_items(items: list) -> str:
    """Formats the JSON list into a readable text block for the AI."""
    if not items:
        return "No items available."

    out_lines = []
    for i, item in enumerate(items[:15]):  # Limit to top 15 to save tokens
        title = item.get("title", "No Title")
        # Use the summary text you already fetched!
        text = item.get("text", "")[:400]
        url = item.get("url", "No URL")
        out_lines.append(
            f"[{i + 1}] HEADLINE: {title}\n    SUMMARY: {text}\n    SOURCE: {url}"
        )
    return "\n\n".join(out_lines)


def process_blob(blob_name: str, storage_client: storage.Client):
    # WRAP ENTIRE LOGIC IN TRY/EXCEPT to ensure thread always dies
    try:
        ticker, date_str = parse_filename(blob_name)
        if not ticker or not date_str:
            return None

        # News output: {ticker}_news_{date_str}.json
        analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_news_{date_str}.json"

        # 1. Read the file
        content = gcs.read_blob(
            config.GCS_BUCKET_NAME, blob_name, client=storage_client
        )
        if not content:
            return None

        try:
            data = json.loads(content)
            stock_news = data.get("stock_news", [])
        except json.JSONDecodeError:
            logging.error(f"[{ticker}] Invalid JSON in {blob_name}")
            return None

        # 2. Format the text for the prompt
        formatted_stock_news = _format_news_items(stock_news)

        # 3. Get Current Date for Grounding
        today_str = datetime.date.today().strftime("%Y-%m-%d")

        # 4. Prompt: "Verify recency with Google"
        prompt = rf"""
You are a news catalyst analyst. Today is **{today_str}**.

Your job is to identify if there is a **fresh, high-impact catalyst** for {ticker} that occurred in the last **48-72 hours**.

### Input Headlines (Potential Catalysts)
{formatted_stock_news}

### Your Mission
1. **CHECK THE DATE:** Compare the "Input Headlines" against Today's Date ({today_str}).
   - If the news is older than 3 days, it is **NOISE** (Score 0.5).
   - Example: If today is Jan 6, and the news is "Oct 22 Earnings", that is ANCIENT HISTORY. Discard it.

2. **VERIFY WITH GOOGLE:**
   - You **MUST** use Google Search to confirm if a headline is actually recent.
   - Search query: "{ticker} news last 2 days".
   - If the headlines provided above are old, but you find *new* breaking news on Google (e.g., today/yesterday), USE THE NEW INFO.

3. **Score:**
   - **0.50:** Noise / Old News / No Catalyst.
   - **>0.70:** CONFIRMED Fresh Bullish Catalyst (Earnings Beat *Yesterday*, Upgrade *Today*, New Contract *Today*).
   - **<0.30:** CONFIRMED Fresh Bearish Catalyst (Miss *Yesterday*, Lawsuit *Today*).

### Output (JSON)
{{example_output}}

### Output — return exactly this JSON
{{
  "score": <float 0.0-1.0>,
  "catalyst_type": "<String e.g. 'Earnings Beat', 'Neutral/Old News', 'Analyst Upgrade'>",
  "analysis": "<Strictly factual paragraph. Start by stating the DATE of the event. If the event was months ago, say 'No recent news; last major event was [Date]'. If fresh, explain why it moves the stock >3% now.>"
}}
"""

        # --- RATE LIMITER: Enforce 1 call every 1.2s across threads ---
        _limiter.wait()

        # We still use generate_with_tools so it CAN search if the text is missing details
        # FAIL FAST: Timeout handled by client init
        response_text, _ = vertex_ai.generate_with_tools(prompt=prompt)

        clean_json_str = _extract_json_object(response_text)
        if not clean_json_str:
            raise ValueError("No JSON object extracted.")

        parsed = json.loads(clean_json_str)
        if "score" in parsed and "analysis" in parsed:
            gcs.write_text(
                config.GCS_BUCKET_NAME,
                analysis_blob_path,
                json.dumps(parsed, indent=2),
                "application/json",
                client=storage_client,
            )
            return analysis_blob_path

    except Exception as e:
        # Catch-all to prevent thread hanging
        logging.error(
            f"[{os.path.basename(blob_name)}] CRITICAL FAIL in process_blob: {e}"
        )
        return None


def run_pipeline():
    logging.info("--- Starting News Catalyst Analysis (Parallel + Throttled) ---")
    storage_client = storage.Client()

    # 1. DELETE ALL FILES UP FRONT (Ensure 1 file per ticker, fresh run)
    try:
        logging.info(f"Deleting all files in output prefix: {OUTPUT_PREFIX}")
        gcs.delete_all_in_prefix(
            config.GCS_BUCKET_NAME, OUTPUT_PREFIX, client=storage_client
        )
    except Exception as e:
        logging.error(f"Failed to clean up output prefix: {e}")

    # 2. List Inputs (Materialize List to Fail Fast)
    logging.info("Listing inputs...")
    try:
        all_inputs = list(
            gcs.list_blobs(
                config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX, client=storage_client
            )
        )
    except Exception as e:
        logging.error(f"Failed to list blobs: {e}")
        return

    if not all_inputs:
        logging.info("No input news files found.")
        return

    total_files = len(all_inputs)
    logging.info(
        f"Processing {total_files} news files with {config.MAX_WORKERS} workers..."
    )

    # 3. Process with ThreadPool (Manual management to skip "wait=True")
    processed_count = 0
    executor = ThreadPoolExecutor(max_workers=config.MAX_WORKERS)
    try:
        future_to_blob = {
            executor.submit(process_blob, item, storage_client): item
            for item in all_inputs
        }

        for i, future in enumerate(as_completed(future_to_blob)):
            try:
                res = future.result()
                if res:
                    processed_count += 1
            except Exception as e:
                logging.error(f"Unknown Thread Failure: {e}")

            # Progress Logging
            if (i + 1) % 50 == 0:
                logging.info(f"Progress: {i + 1}/{total_files} files processed...")
    finally:
        # CRITICAL: Do not wait for zombie threads (e.g. stuck socket close)
        # Force shutdown so the Cloud Function returns '200 OK' immediately.
        logging.info("Forcing executor shutdown (wait=False)...")
        executor.shutdown(wait=False, cancel_futures=True)

    logging.info(
        f"--- News Analysis Finished. Processed {processed_count}/{total_files} files. ---"
    )
