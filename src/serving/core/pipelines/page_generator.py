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
    "title": "Blackstone (BX) Options Trade: Bullish Breakout Signal Detected | ProfitScout",
    "metaDescription": "ProfitScout AI projects a move to $160 for Blackstone (BX). Strong technicals align with $5B Phoenix Financial investment news. See the full call option strategy.",
    "keywords": ["Blackstone options trade", "BX stock forecast", "buy BX calls", "Blackstone private credit news", "BX technical analysis"],
    "h1": "Blackstone (BX) Trade Alert: Bullish Breakout Confirmed"
  },
  "teaser": {
    "signal": "Strongly Bullish outlook with confirming positive momentum",
    "summary": "Blackstone is staging a technical breakout above $146, fueled by a major $5B capital injection from Phoenix Financial.",
    "metrics": {
      "Trend": "Uptrend (Above 50 SMA)",
      "Momentum": "RSI Reset & Rising (Bullish)",
      "Vol": "Low IV Rank (Cheap Premiums)"
    }
  },
  "relatedStocks": ["KKR", "APO", "ARES"],
  "aiOptionsPicks": [
    {
      "strategy": "Buy Call",
      "rationale": "Volatility is underpriced relative to the breakout potential driven by the new capital inflow catalyst.",
      "details": {"expiration": "2025-11-21", "strike": 150, "premium": "3.50 (est)", "impliedVol": "Low"},
      "riskReward": {"maxLoss": "Premium Paid", "breakeven": "153.50", "potential": "Unlimited upside"}
    }
  ],
  "contentBlocks": {
    "thesis": "Detailed paragraph explaining the confluence of the $138 support bounce and the news catalyst.",
    "catalysts": ["$5B Phoenix Financial Investment", "Q3 Fee Earnings Growth (+15%)"],
    "risks": ["Failure to hold $146 support", "Rate hike fears impacting real estate sector"]
  },
  "faq": [
    {
      "question": "Is Blackstone (BX) stock a buy right now?",
      "answer": "ProfitScout's AI model rates BX as 'Strongly Bullish' as of Oct 25, 2025. The stock has reclaimed its 21-day EMA and is supported by rising fee-related earnings."
    },
    {
      "question": "What is the price target for BX?",
      "answer": "Technical analysis suggests an immediate upside target of $160 (recent resistance), with a secondary measured move target of $175 if volume sustains."
    }
  ],
  "internalLinks": [
    {"label": "Compare to KKR Options", "url": "/stocks/KKR"},
    {"label": "Today's Top Financial Sector Trades", "url": "/sectors/financials"}
  ],
  "schemaOrg": {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    "mainEntity": [
      {
        "@type": "Question",
        "name": "Is Blackstone (BX) stock a buy right now?",
        "acceptedAnswer": {
          "@type": "Answer",
          "text": "ProfitScout's AI model rates BX as 'Strongly Bullish'..."
        }
      }
    ]
  }
}
"""

# --- ENHANCED PROMPT: SEO Specialist Persona ---
_PROMPT_TEMPLATE = r"""
You are an elite SEO Strategist and Financial Editor. Your goal is to create a high-ranking content package for the stock **{ticker}** ({company_name}).
We want to capture search traffic for terms like "{ticker} options trade", "{ticker} forecast", and specific questions about the stock's current movement.

### 1. Keyword & Entity Strategy
* **Identify the 'Why':** Why is the stock moving? (e.g. "Earnings Beat", "FDA Approval", "Rate Cut"). Use these semantic terms in your writing.
* **Identify the 'What':** What pattern is forming? (e.g. "Bull Flag", "Oversold Bounce"). Use these technical terms.

### 2. Required JSON Sections
Generate a JSON object matching the exact keys in the example below.

**A. SEO Metadata (`seo`)**
* `title`: Punchy, click-worthy (~60 chars). Pattern: "{company_name} ({ticker}) Options: [Signal] Setup & Targets".
* `metaDescription`: Front-load the catalyst. "AI detects [Signal] for {ticker} driven by [Catalyst]. Targets: [Price]. Trade details inside." (~155 chars).
* `h1`: A strong headline summarizing the trade thesis.

**B. Teaser & Content (`teaser`, `contentBlocks`)**
* `teaser.summary`: 2 sentences connecting the technical setup to the fundamental catalyst.
* `contentBlocks.thesis`: A rich paragraph. Use specific numbers (prices, dates, percentages) from the input text.
* `contentBlocks.catalysts`: Short, punchy bullet points of the specific drivers.

**C. FAQ Schema (`faq`) - CRITICAL FOR SEO**
* Generate 2-3 high-value Questions & Answers based *specifically* on the analysis provided.
* Q1: "Is {ticker} a buy or sell right now?" (Answer using the Signal and Score).
* Q2: "Why is {ticker} stock moving?" (Answer citing the specific news/catalyst).
* Q3: "What is the next price target for {ticker}?" (Answer using the Resistance/Support levels).

**D. Options Strategy (`aiOptionsPicks`)**
* Based on the `Outlook Signal`, suggest a Directional Trade (Call for Bullish, Put for Bearish).
* If Neutral, leave array empty.

**E. Schema.org (`schemaOrg`)**
* Generate a valid `FAQPage` JSON-LD object using the questions from the `faq` section above.

### Input Data
- **Ticker**: {ticker}
- **Date**: {run_date}
- **Signal**: {outlook_signal} {momentum_context}
- **Full Analysis**:
{aggregated_text}

### Output Instructions
Return ONLY the JSON object. No markdown formatting.

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
        momentum_context = rec_data.get("momentum_context", "")
    else:
        outlook_signal = "Neutral"
        momentum_context = ""

    kpi_path = f"{PREP_PREFIX}{ticker}_{run_date_str}.json"
    kpis_json_str = gcs.read_blob(config.GCS_BUCKET_NAME, kpi_path)
    kpis_json = json.loads(kpis_json_str) if kpis_json_str else {}

    prompt = _PROMPT_TEMPLATE.format(
        ticker=ticker,
        company_name=company_name,
        run_date=run_date_str,
        outlook_signal=outlook_signal,
        momentum_context=momentum_context,
        kpis_json=json.dumps(kpis_json, indent=2),
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
            "fullAnalysis": _split_aggregated_text(aggregated_text)
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