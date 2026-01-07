# enrichment/core/pipelines/news_analyzer.py

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from .. import config, gcs
from ..clients import vertex_ai
from google.cloud import bigquery, storage
import os
import re
import json
import datetime

INPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["output"]

# Keeps your existing output format
_EXAMPLE_OUTPUT = """{
  "score": 0.85,
  "catalyst_type": "Strong Bullish Catalyst",
  "analysis": "The latest earnings report confirms a significant acceleration in AI data center revenue, which beat expectations by 15%. Management raised full-year guidance, citing 'unprecedented demand' for the new chip architecture. While macro headwinds persist in the consumer segment, this specific enterprise catalyst is strong enough to drive a breakout."
}"""

def parse_filename(blob_name: str):
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    if match: return (match.group(1), match.group(2))
    return (None, None)

def _extract_json_object(text: str) -> str:
    if not text: return ""
    # Strip code fences
    text = re.sub(r"^\s*```json\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"```\s*$", "", text, flags=re.MULTILINE)
    text = text.strip()
    # Find the JSON bracket boundaries
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start: return text[start : end + 1]
    return text

def _format_news_items(items: list) -> str:
    """Formats the JSON list into a readable text block for the AI."""
    if not items:
        return "No items available."
    
    out_lines = []
    for i, item in enumerate(items[:15]): # Limit to top 15 to save tokens
        title = item.get("title", "No Title")
        # Use the summary text you already fetched!
        text = item.get("text", "")[:400] 
        url = item.get("url", "No URL")
        out_lines.append(f"[{i+1}] HEADLINE: {title}\n    SUMMARY: {text}\n    SOURCE: {url}")
    return "\n\n".join(out_lines)

def process_blob(blob_name: str, storage_client: storage.Client):
    ticker, date_str = parse_filename(blob_name)
    if not ticker or not date_str:
        return None

    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_news_{date_str}.json"
    logging.info(f"[{ticker}] Generating news catalyst analysis for {date_str}...")

    # 1. Read the file
    content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name, client=storage_client)
    if not content:
        return None

    try:
        data = json.loads(content)
        stock_news = data.get("stock_news", [])
        # Macro news is no longer fetched, so we ignore it or treat as empty
    except json.JSONDecodeError:
        logging.error(f"[{ticker}] Invalid JSON in {blob_name}")
        return None
    
    # 2. Format the text for the prompt
    formatted_stock_news = _format_news_items(stock_news)
    
    # 3. Get Current Date for Grounding
    today_str = datetime.date.today().strftime("%Y-%m-%d")

    # 4. Prompt: "Verify recency with Google"
    prompt = r"""
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
""".format(
        ticker=ticker,
        today_str=today_str,
        formatted_stock_news=formatted_stock_news,
        example_output=_EXAMPLE_OUTPUT
    )
    
    try:
        # We still use generate_with_tools so it CAN search if the text is missing details
        response_text, _ = vertex_ai.generate_with_tools(prompt=prompt)
        
        clean_json_str = _extract_json_object(response_text)
        if not clean_json_str:
            raise ValueError("No JSON object extracted.")

        parsed = json.loads(clean_json_str)
        if "score" in parsed and "analysis" in parsed:
            gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, json.dumps(parsed, indent=2), "application/json", client=storage_client)
            return analysis_blob_path
            
    except Exception as e:
        logging.error(f"[{ticker}] Analysis failed: {e}")
        return None

def run_pipeline():
    logging.info("--- Starting News Catalyst Analysis (Live Mode) ---")
    storage_client = storage.Client()

    # Clear old output to ensure fresh analysis
    logging.info(f"Wiping old analysis from: {OUTPUT_PREFIX}")
    gcs.delete_all_in_prefix(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX, client=storage_client)

    all_inputs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX, client=storage_client)
    
    if not all_inputs:
        logging.info("No input news files found.")
        return

    processed_count = 0
    # Use max_workers from config
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        future_to_blob = {
            executor.submit(process_blob, item, storage_client): item
            for item in all_inputs
        }
        for future in as_completed(future_to_blob):
            if future.result():
                processed_count += 1
                
    logging.info(f"--- News Analysis Finished. Processed {processed_count} files. ---")