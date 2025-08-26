# serving/core/pipelines/page_generator.py
import logging
import pandas as pd
import json
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery, storage
from datetime import date
from typing import Dict, Optional, List
from .. import config, gcs
from ..clients import vertex_ai

# --- Define Prefixes ---
INPUT_PREFIX = config.RECOMMENDATION_PREFIX
OUTPUT_PREFIX = config.PAGE_JSON_PREFIX

# Example JSON and Prompt Template remain the same
_EXAMPLE_JSON_FOR_LLM = """
{
  "seo": {
    "title": "ABNB Stock Recommendation August 2025 – AI Bearish Score | ProfitScout",
    "metaDescription": "AI-powered sell signal for ABNB based on Russell 1000 data. Bearish outlook with high valuation risks. Explore full analysis.",
    "keywords": ["ABNB stock recommendation", "Russell 1000 sell signals", "AI stock analysis 2025", "Airbnb bearish score", "ABNB technicals"]
  },
  "teaser": {
    "signal": "SELL",
    "summary": "ABNB shows growth and positive sentiment, but technicals and ratios indicate potential downside. A cautious approach is warranted.",
    "metrics": {
      "peRatio": "Elevated (high valuation)",
      "revenueGrowth": "13% YoY"
    }
  },
  "relatedStocks": ["EXPE", "BKNG"]
}
"""

_PROMPT_TEMPLATE = r"""
You are an SEO and financial analysis expert. Your task is to generate a small, specific JSON object containing SEO metadata, a teaser summary, and related stock tickers. You will base this on the provided weighted score and the full aggregated analysis text.

### Instructions
1.  **Determine the Signal**: Use the `weighted_score` to determine the final recommendation signal.
    * `weighted_score` > 0.68 is "BUY"
    * `weighted_score` between 0.50 and 0.67 is "HOLD"
    * `weighted_score` < 0.50 is "SELL"
2.  **SEO Section**: Based on the signal and the full text, craft a `title`, `metaDescription`, and `keywords` for high search ranking.
3.  **Teaser**:
    * `signal`: Use the signal you determined in step 1.
    * `summary`: Write a new, concise 1-2 sentence summary of the overall outlook from the `aggregated_text`.
    * `metrics`: Extract 2-3 of the most important metrics from the full analysis.
4.  **relatedStocks**: Infer 2-3 public competitor tickers based on the company's "About" section within the `aggregated_text`.
5.  **Format**: Your entire output must be ONLY the JSON object, matching the example's structure.

### Input Data
- **Ticker**: {ticker}
- **Current Year**: {year}
- **Weighted Score**: {weighted_score}
- **Full Aggregated Analysis (Text)**:
{aggregated_text}

### Example Output (Return ONLY this JSON structure)
{example_json}
"""

def _split_aggregated_text(aggregated_text: str) -> Dict[str, str]:
    """Splits aggregated_text into a dictionary of its component sections."""
    sections = re.split(r'\n\n---\n\n', aggregated_text.strip())
    section_dict = {}
    for section in sections:
        match = re.match(r'## (.*?) Analysis\n\n(.*)', section, re.DOTALL)
        if match:
            key = match.group(1).lower().replace(' ', '')
            text = match.group(2).strip()
            key_map = {
                "news": "newsSummary", "technicals": "technicals", "mda": "mdAndA",
                "transcript": "earningsCall", "financials": "financials",
                "metrics": "metrics", "ratios": "ratios"
            }
            final_key = key_map.get(key, key)
            section_dict[final_key] = text
        elif section.startswith("## About"):
            section_dict["about"] = re.sub(r'## About\n\n', '', section).strip()
    return section_dict

def _get_data_from_bq(ticker: str, run_date: str) -> Optional[Dict]:
    """Fetches aggregated_text and weighted_score for a specific ticker and date."""
    try:
        client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
        query = f"""
            SELECT aggregated_text, weighted_score
            FROM `{config.SCORES_TABLE_ID}`
            WHERE ticker = @ticker AND run_date = @run_date
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
                bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
            ]
        )
        df = client.query(query, job_config=job_config).to_dataframe()
        return df.to_dict('records')[0] if not df.empty else None
    except Exception as e:
        logging.error(f"[{ticker}] Failed to fetch BQ data for {run_date}: {e}", exc_info=True)
        return None

def process_blob(blob_name: str) -> Optional[str]:
    """
    Processes one recommendation blob to generate a page JSON using the efficient, two-part method.
    """
    dated_format_regex = re.compile(r'([A-Z\.]+)_recommendation_(\d{4}-\d{2}-\d{2})\.md$')
    file_name = os.path.basename(blob_name)
    match = dated_format_regex.match(file_name)
    if not match:
        logging.warning(f"Skipping non-standard filename in process_blob: {blob_name}")
        return None
    
    ticker, run_date_str = match.groups()
    
    bq_data = _get_data_from_bq(ticker, run_date_str)
    if not bq_data:
        logging.error(f"[{ticker}] Could not find BigQuery data for {run_date_str}. Skipping.")
        return None

    aggregated_text = bq_data.get("aggregated_text")
    weighted_score = bq_data.get("weighted_score")

    full_analysis_sections = _split_aggregated_text(aggregated_text)
    
    # --- FIX IS HERE ---
    # We now correctly create the bullishScore and add it to the final_json dictionary immediately.
    bullish_score = round((weighted_score - 0.5) * 20, 2) if weighted_score is not None else 0.0

    final_json = {
        "symbol": ticker,
        "date": run_date_str,
        "bullishScore": bullish_score,
        "fullAnalysis": full_analysis_sections
    }

    today = date.today()
    prompt = _PROMPT_TEMPLATE.format(
        ticker=ticker,
        year=today.year,
        weighted_score=round(weighted_score, 4),
        aggregated_text=aggregated_text,
        example_json=_EXAMPLE_JSON_FOR_LLM,
    )

    json_blob_path = f"{OUTPUT_PREFIX}{ticker}_page_{run_date_str}.json"
    logging.info(f"[{ticker}] Generating SEO/Teaser JSON for {run_date_str}.")
    
    llm_response_str = ""
    try:
        llm_response_str = vertex_ai.generate(prompt)
        
        if llm_response_str.startswith("```json"):
            llm_response_str = re.search(r'\{.*\}', llm_response_str, re.DOTALL).group(0)

        if not llm_response_str:
            raise ValueError("Received an empty response from the AI model.")
        
        llm_generated_data = json.loads(llm_response_str)
        
        final_json.update(llm_generated_data)

        gcs.write_text(config.GCS_BUCKET_NAME, json_blob_path, json.dumps(final_json, indent=2), "application/json")
        logging.info(f"[{ticker}] Successfully uploaded complete JSON file to {json_blob_path}")
        return json_blob_path
        
    except (json.JSONDecodeError, ValueError, AttributeError) as e:
        logging.error(f"[{ticker}] Failed to generate or parse LLM JSON. Error: {e}. Response was: '{llm_response_str}'")
        return None
    except Exception as e:
        logging.error(f"[{ticker}] An unexpected error occurred in process_blob: {e}", exc_info=True)
        return None

def run_pipeline():
    """Finds recommendation files that are missing a page and processes them."""
    logging.info("--- Starting Page Generation Pipeline (Final Optimized) ---")
    
    all_recommendations = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    all_pages = set(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX))
    
    work_items = []
    dated_format_regex = re.compile(r'([A-Z\.]+)_recommendation_(\d{4}-\d{2}-\d{2})\.md$')

    for rec_path in all_recommendations:
        file_name = os.path.basename(rec_path)
        match = dated_format_regex.match(file_name)
        if not match:
            continue
            
        ticker, run_date_str = match.groups()
        expected_page_path = f"{OUTPUT_PREFIX}{ticker}_page_{run_date_str}.json"

        if expected_page_path not in all_pages:
            work_items.append(rec_path)

    if not work_items:
        logging.info("All recommendations have a corresponding page JSON. No new pages to generate.")
        return

    logging.info(f"Found {len(work_items)} new recommendations to process into pages.")
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS_RECOMMENDER) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    
    logging.info(f"--- Page Generation Pipeline Finished. Processed {count} new pages. ---")