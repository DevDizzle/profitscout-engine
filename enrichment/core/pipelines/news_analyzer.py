import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from .. import config, gcs
from ..clients import vertex_ai
from google.cloud import bigquery
import os
import re
import json

INPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["output"]

# -------- Helpers ------------------------------------------------------------

def parse_filename(blob_name: str):
    """Parses filenames with the format 'TICKER_YYYY-MM-DD.json'."""
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    if match:
        return (match.group(1), match.group(2))  # (ticker, date_str)
    return (None, None)

def _safe_json_loads(s: str):
    try:
        return json.loads(s)
    except Exception:
        return None

# -------- BigQuery metadata lookup ------------------------------------------

def _get_ticker_metadata(bq: bigquery.Client, ticker: str) -> dict:
    """Fetches the latest company metadata for a given ticker."""
    table_id = config.STOCK_METADATA_TABLE_ID
    query = f"""
        SELECT sector, industry, company_name
        FROM `{table_id}`
        WHERE ticker = @ticker
        ORDER BY quarter_end_date DESC
        LIMIT 1
    """
    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
        )
        df = bq.query(query, job_config=job_config).to_dataframe()
        if not df.empty:
            r = df.iloc[0].to_dict()
            return {
                "sector": r.get("sector") or "N/A",
                "industry": r.get("industry") or "N/A",
                "company_name": r.get("company_name") or ticker,
            }
    except Exception as e:
        logging.error(f"[{ticker}] Failed to query metadata: {e}")
    
    return {"sector": "N/A", "industry": "N/A", "company_name": ticker}

# -------- Core processing ----------------------------------------------------

def process_blob(blob_name: str, bq_client: bigquery.Client):
    """Processes one consolidated daily news file for a single ticker."""
    ticker, date_str = parse_filename(blob_name)
    if not ticker or not date_str:
        logging.warning(f"Skipping file with unexpected name format: {blob_name}")
        return None

    meta = _get_ticker_metadata(bq_client, ticker)
    
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_news_{date_str}.json"
    logging.info(f"[{ticker}] Generating news analysis for {date_str}...")

    content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    if not content:
        logging.warning(f"[{ticker}] News file is missing or empty: {blob_name}")
        return None

    news_data = _safe_json_loads(content)
    if not isinstance(news_data, dict):
        logging.error(f"[{ticker}] Could not parse JSON from {blob_name}. Skipping.")
        return None

    # This is the key part - correctly combining the news from your file format
    combined_news = news_data.get("stock_news", []) + news_data.get("macro_news", [])

    if not combined_news:
        neutral = {"score": 0.50, "analysis": "No relevant news was found for this stock today."}
        gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, json.dumps(neutral, indent=2), "application/json")
        return analysis_blob_path

    prompt = r"""
You are a disciplined market-news analyst providing a short-term outlook for an options trader.
Estimate the directional impact on **{ticker} ({company_name})** over the next **1–2 weeks**.

# Context
- Ticker: {ticker}
- Company: {company_name}
- Sector: {sector}
- Industry: {industry}

# Scoring Rules
1.  **No Company-Specific News**: If `ticker_news` is empty in the input, your baseline score is **0.50**. Adjust this score only slightly (e.g., 0.45-0.55) based on the direct relevance of any macro news.
2.  **Company-Specific News Exists**: If `ticker_news` is present, it is the primary driver of your score.
3.  **Magnitude is Key**: Earnings, M&A, and guidance are far more important than minor updates.

# Input Data
{news_json}

# Output — return exactly this JSON, nothing else
{{
  "score": <float between 0 and 1>,
  "analysis": "<Your dense, 150-220 word analysis>"
}}
""".format(
        ticker=ticker,
        company_name=meta.get("company_name"),
        sector=meta.get("sector"),
        industry=meta.get("industry"),
        news_json=json.dumps(news_data, ensure_ascii=False, indent=2),
    )

    try:
        raw_response = vertex_ai.generate(prompt)
        json_match = re.search(r'\{.*\}', raw_response, re.DOTALL)
        
        if not json_match:
            logging.error(f"[{ticker}] No JSON object found in the model's response. Raw response: '{raw_response}'")
            return None

        json_string = json_match.group(0)
        parsed_json = json.loads(json_string)

        if "score" in parsed_json and "analysis" in parsed_json:
            gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, json.dumps(parsed_json, indent=2), "application/json")
            return analysis_blob_path
        else:
            logging.error(f"[{ticker}] Parsed JSON is missing 'score' or 'analysis' keys. Parsed JSON: {parsed_json}")
            return None

    except json.JSONDecodeError:
        logging.error(f"[{ticker}] Failed to decode JSON from the model's response. Raw response: '{raw_response}'")
        return None
    except Exception as e:
        logging.error(f"[{ticker}] An unexpected error occurred during model call or processing: {e}", exc_info=True)
        return None

def run_pipeline():
    """Main pipeline entry point with robust timeout handling."""
    logging.info("--- Starting News Analysis Pipeline (with worker timeouts) ---")
    work_items = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    if not work_items:
        logging.info("No new news files to process.")
        return

    bq_client = bigquery.Client()
    processed_count = 0
    
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        future_to_blob = {
            executor.submit(process_blob, item, bq_client): item 
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