# serving/core/pipelines/page_generator.py
import logging
import pandas as pd
import json
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from datetime import date
from typing import Dict, Optional

from .. import config, gcs
from .. import bq
from ..clients import vertex_ai

INPUT_PREFIX = config.RECOMMENDATION_PREFIX
OUTPUT_PREFIX = config.PAGE_JSON_PREFIX
PREP_PREFIX = 'prep/'

# --- SEO-Optimized Example Output ---
# CRITICAL: This example enforces the schema your web app needs.
# We added 'faq' here so your frontend can render an Accordion component.
_EXAMPLE_JSON_FOR_LLM = """
{
  "seo": {
    "title": "Blackstone (BX) Options Flow: Bullish Gamma Setup & Targets | ProfitScout",
    "metaDescription": "ProfitScout AI projects a move to $160 for Blackstone (BX). Strong call buying at $150 strike suggests institutional positioning. See the full gamma analysis.",
    "keywords": ["Blackstone options trade", "BX stock forecast", "buy BX calls", "Blackstone private credit news", "BX gamma exposure"],
    "h1": "Blackstone (BX) Call Wall Test: Breakout Imminent"
  },
  "analystBrief": {
    "headline": "Bulls attacking the $150 Call Wall",
    "content": "<p>Today's order flow for <strong>Blackstone (BX)</strong> reveals a distinct <strong>bullish gamma setup</strong>. We are tracking significant <strong>unusual call volume</strong> at the $150 strike, which currently acts as the primary <strong>Call Wall</strong>. A clean break above this level could trigger a dealer hedging event (Gamma Squeeze), accelerating the move toward $160.</p><p>Conversely, support is firm at the $140 <strong>Put Wall</strong>. With the <strong>Put/Call Ratio</strong> at 0.65, sentiment is aggressively bullish, suggesting traders are positioning for an upside surprise driven by the recent Phoenix Financial news.</p>"
  },
  "tradeSetup": {
    "signal": "Bullish",
    "confidence": "High",
    "strategy": "Long Calls",
    "catalyst": "Gamma Squeeze Potential + News"
  },
  "faq": [
    {
      "question": "What is the max pain for Blackstone (BX)?",
      "answer": "The current Max Pain level is $145. However, heavy call buying at $150 suggests traders expect the price to pin higher by expiry."
    },
    {
      "question": "Is BX seeing unusual options activity?",
      "answer": "Yes, we detected unusual flow in the Nov $150 Calls, with volume exceeding open interest by 2x, indicating fresh institutional entry."
    }
  ]
}
"""

# --- ENHANCED PROMPT: SEO Specialist Persona ---
_PROMPT_TEMPLATE = r"""
You are a Senior Derivatives Analyst and SEO Specialist. Your job is to write a high-impact "Daily Options Brief" for {ticker} ({company_name}).
Your audience consists of traders looking for "Gamma Squeezes", "Unusual Flow", and "Vol Windows".

### Input Data
- **Ticker**: {ticker}
- **Date**: {run_date}
- **Signal**: {outlook_signal}
- **Market Structure (The "Greeks"):**
{options_context}
- **News & Technicals:**
{aggregated_text}

### Instructions
Generate a JSON object matching the exact keys in the example below.

**1. SEO Metadata (`seo`)**
* `title`: "{ticker} Options Flow: [Bullish/Bearish] Gamma Setup & Targets"
* `h1`: Strong headline focusing on the directional bias (e.g., "{ticker} Call Wall Test: Breakout Imminent").

**2. Analyst Brief (`analystBrief`)**
* `headline`: A punchy sub-headline summarizing the immediate setup (e.g., "Bulls attacking the ${{call_wall}} Call Wall").
* `content`: A ~250 word HTML-formatted analysis (<p>, <strong>).
    * **CRITICAL:** You MUST reference the specific data from "Market Structure".
    * Analyze the **Call Wall** (Resistance) and **Put Wall** (Support).
    * Interpret the **Put/Call Ratio** and **Net Gamma** (e.g., "Negative Gamma regime implies higher volatility").
    * Cite the **Top Active Contracts** as evidence of "Smart Money" positioning.
    * Use professional lexicon: "Dealer Hedging", "Implied Volatility", "Vanna", "Oversold".

**3. Trade Setup (`tradeSetup`)**
* `signal`: The directional bias ("Bullish", "Bearish", "Neutral").
* `confidence`: "High", "Medium", "Low".
* `strategy`: Specific trade type (e.g., "Long Calls", "Debit Spread", "Cash Secured Puts").
* `catalyst`: The primary fundamental or technical driver.

**4. FAQ (`faq`)**
* Generate 2 SEO-rich Q&As (e.g., "What is the max pain for {ticker}?", "Is {ticker} seeing unusual call volume?").

### Example JSON Structure
{example_json}
"""

def _clean_aggregated_text(text: str) -> str:
    if not text or not isinstance(text, str):
        return ""
    return text.replace('\\"', "'")

def _split_aggregated_text(aggregated_text: str) -> Dict[str, str]:
    sections = re.split(r'\n\n---\n\n', aggregated_text.strip())
    section_dict = {}
    for section in sections:
        match = re.match(r'## (.*?) Analysis\n\n(.*)', section, re.DOTALL)
        if match:
            key = match.group(1).lower().replace(' ', '')
            text = match.group(2).strip()
            section_dict[key] = text
    return section_dict

def _get_data_from_bq(ticker: str, run_date: str) -> Optional[Dict]:
    """Fetches analysis text and scores from BigQuery."""
    try:
        client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
        query = f"""
            SELECT
                t1.aggregated_text,
                t1.weighted_score,
                t2.company_name
            FROM `{config.SCORES_TABLE_ID}` AS t1
            LEFT JOIN `{config.BUNDLER_STOCK_METADATA_TABLE_ID}` AS t2
                ON t1.ticker = t2.ticker
            WHERE t1.ticker = @ticker AND t1.run_date = @run_date
            ORDER BY t2.quarter_end_date DESC
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
        logging.error(f"[{ticker}] BQ Fetch failed: {e}")
        return None

def _delete_old_page_files(ticker: str):
    prefix = f"{OUTPUT_PREFIX}{ticker}_page_"
    blobs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix)
    for blob in blobs:
        gcs.delete_blob(config.GCS_BUCKET_NAME, blob)

def process_blob(blob_name: str) -> Optional[str]:
    dated_format_regex = re.compile(r'([A-Z\.]+)_recommendation_(\d{4}-\d{2}-\d{2})\.md$')
    file_name = os.path.basename(blob_name)
    match = dated_format_regex.match(file_name)
    if not match:
        return None

    ticker, run_date_str = match.groups()

    bq_data = _get_data_from_bq(ticker, run_date_str)
    if not bq_data:
        logging.error(f"[{ticker}] No data in BigQuery for {run_date_str}")
        return None

    aggregated_text = _clean_aggregated_text(bq_data.get("aggregated_text"))
    weighted_score = bq_data.get("weighted_score", 0.5)
    company_name = bq_data.get("company_name", ticker)

    bullish_score = round((weighted_score - 0.5) * 20 + 50, 2) 

    rec_json_path = blob_name.replace('.md', '.json')
    rec_json_str = gcs.read_blob(config.GCS_BUCKET_NAME, rec_json_path)
    if rec_json_str:
        rec_data = json.loads(rec_json_str)
        outlook_signal = rec_data.get("outlook_signal", "Neutral")
    else:
        outlook_signal = "Neutral"

    # --- NEW: Fetch Options Market Structure ---
    options_market_structure = bq.fetch_options_market_structure(ticker)
    
    prompt = _PROMPT_TEMPLATE.format(
        ticker=ticker,
        company_name=company_name,
        run_date=run_date_str,
        outlook_signal=outlook_signal,
        options_context=json.dumps(options_market_structure, indent=2),
        aggregated_text=aggregated_text,
        example_json=_EXAMPLE_JSON_FOR_LLM,
    )

    try:
        llm_response = vertex_ai.generate(prompt)
        clean_json = llm_response.replace("```json", "").replace("```", "").strip()
        generated_data = json.loads(clean_json)

        final_json = {
            "symbol": ticker,
            "date": run_date_str,
            "bullishScore": bullish_score,
            "fullAnalysis": _split_aggregated_text(aggregated_text),
            "marketStructure": options_market_structure # Pass raw data to frontend
        }
        final_json.update(generated_data)

        output_path = f"{OUTPUT_PREFIX}{ticker}_page_{run_date_str}.json"
        _delete_old_page_files(ticker)
        gcs.write_text(config.GCS_BUCKET_NAME, output_path, json.dumps(final_json, indent=2), "application/json")
        
        logging.info(f"[{ticker}] Generated SEO Page JSON: {output_path}")
        return output_path

    except Exception as e:
        logging.error(f"[{ticker}] Page Gen Failed: {e}", exc_info=True)
        return None

def run_pipeline():
    logging.info("--- Starting Page Generation Pipeline (SEO Optimized) ---")
    work_items = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    
    if not work_items:
        return

    processed_count = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS_RECOMMENDER) as executor:
        futures = {executor.submit(process_blob, item) for item in work_items}
        for future in as_completed(futures):
            if future.result():
                processed_count += 1

    logging.info(f"--- Page Generation Finished. Processed {processed_count} pages. ---")