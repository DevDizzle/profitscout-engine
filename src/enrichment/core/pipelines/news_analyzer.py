# enrichment/core/pipelines/news_analyzer.py

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from .. import config, gcs
from ..clients import vertex_ai
from google.cloud import bigquery, storage
import os
import re
import json

# --- Configuration ---
INPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["output"]

# --- Example remains bearish to ensure model can handle puts ---
_EXAMPLE_OUTPUT = """{
  "score": 0.12,
  "catalyst_type": "Strong Bearish Catalyst",
  "analysis": "A detailed review of the news regarding the product recall reveals significant safety concerns that will necessitate a costly replacement program. The company's full-year guidance was also revised downward in the press release, citing supply chain issues and the financial impact of the recall. This combination of negative events is a severe catalyst that will likely lead to analyst downgrades and a sharp price decline. This setup is a prime candidate for buying put options to capture the expected downward volatility."
}"""

# -------- Helpers (No changes needed) ----------------------------------------
def parse_filename(blob_name: str):
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    if match: return (match.group(1), match.group(2))
    return (None, None)

def _extract_json_object(text: str) -> str:
    if not text: return ""
    text = re.sub(r"^\s*```json\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"```\s*$", "", text, flags=re.MULTILINE)
    text = text.strip()
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start: return text[start : end + 1]
    return text

# -------- Metadata & BigQuery Helpers ----------------------------------------
def _get_ticker_metadata(bq: bigquery.Client, ticker: str) -> dict:
    table_id = config.STOCK_METADATA_TABLE_ID
    query = f"SELECT sector, industry, company_name FROM `{table_id}` WHERE ticker = @ticker ORDER BY quarter_end_date DESC LIMIT 1"
    try:
        job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)])
        df = bq.query(query, job_config=job_config).to_dataframe()
        if not df.empty:
            r = df.iloc[0].to_dict()
            return {"sector": r.get("sector") or "N/A", "industry": r.get("industry") or "N/A", "company_name": r.get("company_name") or ticker}
    except Exception as e:
        logging.error(f"[{ticker}] Failed to query metadata: {e}")
    return {"sector": "N/A", "industry": "N/A", "company_name": ticker}

# -------- Core processing ----------------------------------------------------
def process_blob(blob_name: str, bq_client: bigquery.Client, storage_client: storage.Client):
    ticker, date_str = parse_filename(blob_name)
    if not ticker or not date_str:
        return None

    # Retrieve metadata
    meta = _get_ticker_metadata(bq_client, ticker)
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_news_{date_str}.json"
    logging.info(f"[{ticker}] Generating news catalyst analysis for {date_str}...")

    # Pass the shared storage_client to avoid creating a new one (fixes SSL errors)
    content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name, client=storage_client)
    
    # Fix for crashes on invalid input data
    if not content:
        logging.warning(f"[{ticker}] Content was empty for {blob_name}")
        return None

    try:
        news_data = json.loads(content)
    except json.JSONDecodeError as e:
        logging.error(f"[{ticker}] Invalid JSON in {blob_name}: {e}")
        return None
    
    # Extract URLs from the input JSON
    news_urls = [
        item['url'] for item in news_data.get('stock_news', []) if item.get('url')
    ]
    urls_for_prompt = "\n".join(news_urls) if news_urls else "No URLs provided in the input file."

    # --- Prompt for Browse-First Logic ---
    prompt = r"""
You are a news catalyst analyst for a directional options trader who BUYS premium. Your goal is to determine if there is a high-impact news catalyst that could cause a sharp, volatile price move (>2-5%) for {ticker}. You are equipped with a web browser.

# Your Task & Information Sources (in order of priority)
1.  **Browse Provided URLs (Primary Task):** Your most important task is to BROWSE the URLs listed below. Read the full content of these articles to find specific, material information about {ticker}.
2.  **Real-Time Google Search (Secondary Task):** After browsing, use your search tool to find any other significant news about {ticker} that might have been missed.
3.  **Macro Alignment:** From the news you browse (and any search results), identify the major macro themes involved—examples: Fed policy path, inflation trends, energy prices, credit conditions, geopolitical shocks, consumer demand. Explain whether the stock-specific catalyst is amplified or muted by those broader themes.

# CRITICAL FORMATTING RULES
1.  **NO DOUBLE QUOTES**: Use single quotes (') for ALL quoted text, names, or emphasis inside the JSON values.
2.  **NO BACKSLASHES**: Do not use backslashes.
3.  **VALID JSON**: Ensure the output is valid, parsable JSON.

# Analysis Rules
1.  **Find the Material Catalyst**: Base your analysis on the detailed information you discover from browsing and searching. A simple headline is not enough.
2.  **No Catalyst = AVOID**: If, after browsing the URLs and searching, you find no significant, stock-moving news (like earnings, guidance, M&A, etc.), you MUST output a score near 0.5.
3.  **Be Specific**: Your analysis must cite details you found in the articles.

# URLs to Browse First
{urls_to_browse}

# Context
- Ticker: {ticker}
- Company: {company_name}
- Sector: {sector}

# Example Output (for format only; do not copy wording)
{example_output}

# Output — return exactly this JSON, nothing else
{{
  "score": <float, 0.0-0.2 (Strong Bearish Catalyst), 0.2-0.4 (Mild Bearish), 0.4-0.6 (NO CATALYST - AVOID), 0.6-0.8 (Mild Bullish), 0.8-1.0 (Strong Bullish Catalyst)>,
  "catalyst_type": "<'Strong Bearish Catalyst', 'Mild Bearish Catalyst', 'Neutral / No Catalyst', 'Mild Bullish Catalyst', 'Strong Bullish Catalyst'>",
  "analysis": "<One dense paragraph (150-220 words) explaining the catalyst you found from browsing the URLs, citing article specifics, and explicitly stating how it aligns or conflicts with the dominant macro themes you identified (e.g., tightening credit, slowing consumer demand, falling commodity prices). Discuss why that alignment changes the expected price volatility. If no catalyst exists, state that clearly.>"
}}
""".format(
        ticker=ticker,
        company_name=meta.get("company_name"),
        sector=meta.get("sector"),
        urls_to_browse=urls_for_prompt,
        example_output=_EXAMPLE_OUTPUT
    )
    
    try:
        # REVERTED: Call generate_with_tools without model overrides to use default Flash model
        response_text, _ = vertex_ai.generate_with_tools(prompt=prompt)
        
        clean_json_str = _extract_json_object(response_text)
        if not clean_json_str:
            raise ValueError("No JSON object could be extracted from model response.")

        parsed_json = json.loads(clean_json_str)

        if all(k in parsed_json for k in ["score", "analysis", "catalyst_type"]):
            # Pass shared client to write operation
            gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, json.dumps(parsed_json, indent=2), "application/json", client=storage_client)
            return analysis_blob_path
        else:
            logging.error(f"[{ticker}] Parsed JSON is missing required keys. Parsed JSON: {parsed_json}")
            return None

    except Exception as e:
        logging.error(f"[{ticker}] An unexpected error occurred: {e}", exc_info=True)
        return None

def run_pipeline():
    logging.info("--- Starting News Catalyst Analysis Pipeline (Incremental) ---")
    
    # Init shared clients ONCE
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    # 1. Get list of all input files
    all_inputs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX, client=storage_client)
    
    # 2. Get list of all existing output files
    # Optimization: Removing the 'delete_all' call allows us to skip already processed files
    # gcs.delete_all_in_prefix(bucket_name=config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX, client=storage_client) 
    existing_outputs = set(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX, client=storage_client))

    # 3. Filter work items
    work_items = []
    for blob_name in all_inputs:
        # Predict the output name based on input logic
        ticker, date_str = parse_filename(blob_name)
        if ticker and date_str:
            expected_output = f"{OUTPUT_PREFIX}{ticker}_news_{date_str}.json"
            if expected_output not in existing_outputs:
                work_items.append(blob_name)

    if not work_items:
        logging.info("No new news files to process (all up-to-date).")
        return

    logging.info(f"Found {len(work_items)} new files to process (skipped {len(all_inputs) - len(work_items)} existing).")

    processed_count = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        future_to_blob = {
            executor.submit(process_blob, item, bq_client, storage_client): item
            for item in work_items
        }
        for future in as_completed(future_to_blob):
            blob_name = future_to_blob[future]
            try:
                result = future.result(timeout=config.WORKER_TIMEOUT)
                if result:
                    processed_count += 1
            except TimeoutError:
                logging.error(f"TIMEOUT: Processing for {blob_name} took longer than {config.WORKER_TIMEOUT}s and was skipped.")
            except Exception as exc:
                logging.error(f"ERROR: Processing {blob_name} failed with an exception: {exc}", exc_info=True)
                
    logging.info(f"--- News Analysis Pipeline Finished. Successfully processed {processed_count} files. ---")