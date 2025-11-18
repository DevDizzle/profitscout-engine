# enrichment/core/pipelines/news_analyzer.py

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from .. import config, gcs
from ..clients import vertex_ai
from google.cloud import bigquery
import os
import re
import json
import datetime as dt

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

# -------- Worldview & BigQuery Helpers (No changes needed) --------------------
def _get_macro_worldview() -> str:
    try:
        blob_name = config.macro_thesis_blob_name() 
        content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
        if content:
            worldview_data = json.loads(content)
            return worldview_data.get("worldview", "Macro worldview not available.")
    except Exception as e:
        logging.error(f"Failed to load macro worldview: {e}")
    return "Macro worldview context is currently unavailable. Analyze news on its own merit."

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
def process_blob(blob_name: str, bq_client: bigquery.Client, macro_worldview: str):
    ticker, date_str = parse_filename(blob_name)
    if not ticker or not date_str:
        return None

    meta = _get_ticker_metadata(bq_client, ticker)
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_news_{date_str}.json"
    logging.info(f"[{ticker}] Generating news catalyst analysis for {date_str}...")

    content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    news_data = json.loads(content) if content else {}
    
    # --- NEW: Extract URLs from the input JSON ---
    news_urls = [
        item['url'] for item in news_data.get('stock_news', []) if item.get('url')
    ]
    urls_for_prompt = "\n".join(news_urls) if news_urls else "No URLs provided in the input file."

    # --- ENTIRE PROMPT IS REWRITTEN FOR BROWSE-FIRST LOGIC ---
    prompt = r"""
You are a news catalyst analyst for a directional options trader who BUYS premium. Your goal is to determine if there is a high-impact news catalyst that could cause a sharp, volatile price move (>2-5%) for {ticker}. You are equipped with a web browser.

# Your Task & Information Sources (in order of priority)
1.  **Browse Provided URLs (Primary Task):** Your most important task is to BROWSE the URLs listed below. Read the full content of these articles to find specific, material information about {ticker}.
2.  **Real-Time Google Search (Secondary Task):** After browsing, use your search tool to find any other significant news about {ticker} that might have been missed.
3.  **Synthesize with Macro Context:** Integrate your findings with the provided `Macro Worldview` to gauge if the news will be amplified or muted by the market.

# Analysis Rules
1.  **Find the Material Catalyst**: Base your analysis on the detailed information you discover from browsing and searching. A simple headline is not enough.
2.  **No Catalyst = AVOID**: If, after browsing the URLs and searching, you find no significant, stock-moving news (like earnings, guidance, M&A, etc.), you MUST output a score near 0.5.
3.  **Be Specific**: Your analysis must cite details you found in the articles.

# URLs to Browse First
{urls_to_browse}

# Macro Worldview (for context)
"{macro_worldview}"

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
  "analysis": "<One dense paragraph (150-220 words) explaining the catalyst you found from browsing the URLs, how the macro view affects it, and the potential impact on price volatility. If no catalyst exists, state that clearly.>"
}}
""".format(
        ticker=ticker,
        company_name=meta.get("company_name"),
        sector=meta.get("sector"),
        urls_to_browse=urls_for_prompt,
        macro_worldview=macro_worldview,
        example_output=_EXAMPLE_OUTPUT
    )
    
    try:
        # This function must be capable of both browsing and searching based on the prompt's instructions.
        response_text, _ = vertex_ai.generate_with_tools(
            prompt=prompt,
            model_name=getattr(config, "NEWS_ANALYZER_MODEL_NAME", config.MODEL_NAME),
            temperature=getattr(config, "NEWS_ANALYZER_TEMPERATURE", config.TEMPERATURE),
        )
        
        clean_json_str = _extract_json_object(response_text)
        if not clean_json_str:
            raise ValueError("No JSON object could be extracted from model response.")

        parsed_json = json.loads(clean_json_str)

        if all(k in parsed_json for k in ["score", "analysis", "catalyst_type"]):
            gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, json.dumps(parsed_json, indent=2), "application/json")
            return analysis_blob_path
        else:
            logging.error(f"[{ticker}] Parsed JSON is missing required keys. Parsed JSON: {parsed_json}")
            return None

    except Exception as e:
        logging.error(f"[{ticker}] An unexpected error occurred: {e}", exc_info=True)
        return None

def run_pipeline():
    logging.info("--- Starting News Catalyst Analysis Pipeline (with URL Browsing) ---")
    logging.info("Fetching latest macro worldview...")
    macro_worldview = _get_macro_worldview()
    logging.info("Macro worldview loaded.")
    logging.info(f"Clearing output directory: gs://{config.GCS_BUCKET_NAME}/{OUTPUT_PREFIX}")
    gcs.delete_all_in_prefix(bucket_name=config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX)
    work_items = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    if not work_items:
        logging.info("No new news files to process.")
        return
    bq_client = bigquery.Client()
    processed_count = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        future_to_blob = {
            executor.submit(process_blob, item, bq_client, macro_worldview): item 
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
    logging.info(f"--- News Analysis Pipeline Finished. Successfully processed {processed_count} of {len(work_items)} files. ---")