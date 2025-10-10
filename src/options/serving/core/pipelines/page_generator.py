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
PREP_PREFIX = "prep/"  # For KPI JSON path

# --- Updated Example (includes aiOptionsPicks) ---
_EXAMPLE_JSON_FOR_LLM = """
{
  "seo": {
    "title": "Is Expedia Group (EXPE) Breaking Out Right Now? | ProfitScout",
    "metaDescription": "AI-powered analysis signals a momentum-led BUY for Expedia Group (EXPE), with strong technicals, favorable news flow, and supportive earnings tone. Explore the full stock analysis.",
    "keywords": ["Expedia Group stock", "EXPE stock analysis", "Is EXPE a buy", "AI stock signals 2025", "EXPE technicals"]
  },
  "teaser": {
    "signal": "Moderately Bullish outlook encountering short-term weakness",
    "summary": "EXPE shows a momentum breakout supported by positive headlines and stable fundamentals.",
    "metrics": {
      "Price Trend": "Uptrend with higher highs",
      "Volume Confirmation": "Above-average volume on advances",
      "Guidance Tone": "Improving quarter over quarter"
    }
  },
  "relatedStocks": ["BKNG", "ABNB", "TRIP"],
  "aiOptionsPicks": [
    {
      "strategy": "Buy Call",
      "rationale": "Bullish outlook with positive momentum supports upside bet.",
      "details": {"expiration": "2025-10-18", "strike": 150, "premium": "5.00 (est)", "impliedVol": "30% (from hist vol)"},
      "riskReward": {"maxLoss": "Premium paid", "breakeven": "Strike + premium", "potential": "Profits if stock rises"}
    }
  ]
}
"""

# --- Updated Prompt ---
_PROMPT_TEMPLATE = r"""
You are an expert financial copywriter and SEO analyst specializing in AI-powered options trades. Your task is to generate a JSON object with SEO metadata, a teaser, related stocks, and AI-curated options picks based on the provided analysis.

### Signal Policy (Crucial)
- **Use the Provided Signals**: Your primary directive is to use the `Outlook Signal` and `Momentum Context` provided below.
- **Combine for Teaser**: The `teaser.signal` MUST be a direct combination of the `Outlook Signal` and the `Momentum Context`. For example: "Strongly Bullish outlook with confirming positive momentum."
- **Momentum-First Narrative**: Prioritize technicals and KPIs (trend, RSI, volatility) for your rationale, especially for the `aiOptionsPicks`.

### Options Focus
- **Directional Trades**: A "Bullish" `Outlook Signal` should lead to "Buy Call" strategies. A "Bearish" `Outlook Signal` should lead to "Buy Put" strategies. A "Neutral" signal should result in an empty `aiOptionsPicks` array.
- **Rationale**: The rationale for each pick must tie back to the provided KPIs and analysis.
- **Hedged Trades (Advanced)**:
    - If a "Bullish" outlook has "short-term weakness," you can suggest a secondary, speculative "Buy Put" as a hedge.
    - If a "Bearish" outlook has a "short-term rally," you can suggest a secondary, speculative "Buy Call" to play the bounce.

### Instructions
1) **SEO Title**: Create a compelling, 60-70 character title that includes the company name/ticker and an options or outlook angle. End with "| ProfitScout".
2) **Teaser**:
    - `signal`: Combine the `Outlook Signal` and `Momentum Context`.
    - `summary`: Write a 1-2 sentence summary with a clear options hint.
    - `metrics`: List exactly three key metrics from the provided KPIs or analysis.
3) **Related Stocks**: Identify 2-3 competitors from the "About" section.
4) **aiOptionsPicks**: Generate an array of 0-2 options picks. Each pick should include `strategy`, `rationale`, `details` (with placeholders for strike, premium, etc.), and `riskReward`.

### Input Data
- **Ticker**: {ticker}
- **Company Name**: {company_name}
- **Current Year**: {year}
- **Outlook Signal**: {outlook_signal}
- **Momentum Context**: {momentum_context}
- **KPIs (Dashboard Metrics)**: {kpis_json}
- **Recommendation MD (Outlook)**: {recommendation_md}
- **Full Aggregated Analysis**: {aggregated_text}

### Example Output (JSON only)
{example_json}
"""


def _clean_aggregated_text(text: str) -> str:
    """
    A simple cleaning function to replace escaped double quotes with single quotes.
    """
    if not text or not isinstance(text, str):
        return ""
    return text.replace('\\"', "'")


def _split_aggregated_text(aggregated_text: str) -> Dict[str, str]:
    """Splits aggregated_text into a dictionary of its component sections."""
    sections = re.split(r"\n\n---\n\n", aggregated_text.strip())
    section_dict = {}
    for section in sections:
        match = re.match(r"## (.*?) Analysis\n\n(.*)", section, re.DOTALL)
        if match:
            key = match.group(1).lower().replace(" ", "")
            text = match.group(2).strip()
            key_map = {
                "news": "newsSummary",
                "technicals": "technicals",
                "mda": "mdAndA",
                "transcript": "earningsCall",
                "financials": "financials",
                "fundamentals": "fundamentals",
            }
            final_key = key_map.get(key, key)
            section_dict[final_key] = text
        elif section.startswith("## About"):
            section_dict["about"] = re.sub(r"## About\n\n", "", section).strip()
    return section_dict


def _get_data_from_bq(ticker: str, run_date: str) -> Optional[Dict]:
    """
    Fetches aggregated_text, weighted_score, and company_name for a specific ticker and date.
    """
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
        return df.to_dict("records")[0] if not df.empty else None
    except Exception as e:
        logging.error(
            f"[{ticker}] Failed to fetch BQ data for {run_date}: {e}", exc_info=True
        )
        return None


def _delete_old_page_files(ticker: str):
    """Deletes all previous page JSON files for a given ticker."""
    prefix = f"{OUTPUT_PREFIX}{ticker}_page_"
    blobs_to_delete = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix)
    for blob_name in blobs_to_delete:
        try:
            gcs.delete_blob(config.GCS_BUCKET_NAME, blob_name)
        except Exception as e:
            logging.error(f"[{ticker}] Failed to delete old page file {blob_name}: {e}")


def process_blob(blob_name: str) -> Optional[str]:
    """
    Processes one recommendation blob to generate a page JSON.
    """
    dated_format_regex = re.compile(
        r"([A-Z\.]+)_recommendation_(\d{4}-\d{2}-\d{2})\.md$"
    )
    file_name = os.path.basename(blob_name)
    match = dated_format_regex.match(file_name)
    if not match:
        return None

    ticker, run_date_str = match.groups()

    bq_data = _get_data_from_bq(ticker, run_date_str)
    if not bq_data or not bq_data.get("company_name"):
        logging.error(
            f"[{ticker}] Could not find BQ data or company name for {run_date_str}."
        )
        return None

    # --- THIS IS THE FIX ---
    # Clean the aggregated_text immediately after fetching it.
    aggregated_text = _clean_aggregated_text(bq_data.get("aggregated_text"))
    weighted_score = bq_data.get("weighted_score")
    company_name = bq_data.get("company_name")

    full_analysis_sections = _split_aggregated_text(aggregated_text)

    bullish_score = (
        round((weighted_score - 0.5) * 20, 2) if weighted_score is not None else 0.0
    )

    final_json = {
        "symbol": ticker,
        "date": run_date_str,
        "bullishScore": bullish_score,
        "fullAnalysis": full_analysis_sections,
    }

    recommendation_json_path = blob_name.replace(".md", ".json")
    recommendation_md = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    try:
        rec_json_str = gcs.read_blob(config.GCS_BUCKET_NAME, recommendation_json_path)
        rec_data = json.loads(rec_json_str) if rec_json_str else {}
        outlook_signal = rec_data.get("outlook_signal", "Neutral / Mixed")
        momentum_context = rec_data.get("momentum_context", "")
    except Exception as e:
        logging.error(f"[{ticker}] Failed to fetch or parse recommendation JSON: {e}")
        outlook_signal = "Neutral / Mixed"
        momentum_context = ""

    kpi_path = f"{PREP_PREFIX}{ticker}_{run_date_str}.json"
    try:
        kpis_json_str = gcs.read_blob(config.GCS_BUCKET_NAME, kpi_path)
        kpis_json = json.loads(kpis_json_str) if kpis_json_str else {}
    except Exception as e:
        logging.error(f"[{ticker}] Failed to fetch or parse KPI JSON: {e}")
        kpis_json = {}

    prompt = _PROMPT_TEMPLATE.format(
        ticker=ticker,
        company_name=company_name,
        year=date.today().year,
        outlook_signal=outlook_signal,
        momentum_context=momentum_context,
        kpis_json=json.dumps(kpis_json, indent=2),
        recommendation_md=recommendation_md,
        aggregated_text=aggregated_text,
        example_json=_EXAMPLE_JSON_FOR_LLM,
    )

    json_blob_path = f"{OUTPUT_PREFIX}{ticker}_page_{run_date_str}.json"
    logging.info(f"[{ticker}] Generating SEO/Teaser/Options JSON for {run_date_str}.")

    llm_response_str = ""
    try:
        llm_response_str = vertex_ai.generate(prompt)

        if llm_response_str.strip().startswith("```json"):
            match = re.search(r"\{.*\}", llm_response_str, re.DOTALL)
            if match:
                llm_response_str = match.group(0)

        llm_generated_data = json.loads(llm_response_str)
        final_json.update(llm_generated_data)

        _delete_old_page_files(ticker)
        gcs.write_text(
            config.GCS_BUCKET_NAME,
            json_blob_path,
            json.dumps(final_json, indent=2),
            "application/json",
        )
        logging.info(
            f"[{ticker}] Successfully uploaded complete JSON file to {json_blob_path}"
        )
        return json_blob_path

    except (json.JSONDecodeError, ValueError, AttributeError) as e:
        logging.error(
            f"[{ticker}] Failed to generate/parse LLM JSON. Error: {e}. Response: '{llm_response_str}'"
        )
        return None
    except Exception as e:
        logging.error(f"[{ticker}] An unexpected error occurred: {e}", exc_info=True)
        return None


def run_pipeline():
    """
    Finds all available recommendations and generates a fresh page JSON for each.
    """
    logging.info("--- Starting Page Generation Pipeline ---")

    work_items = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)

    if not work_items:
        logging.info("No recommendation files found to process.")
        return

    logging.info(f"Found {len(work_items)} recommendations to process into pages.")

    processed_count = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS_RECOMMENDER) as executor:
        futures = {executor.submit(process_blob, item) for item in work_items}
        for future in as_completed(futures):
            if future.result():
                processed_count += 1

    logging.info(
        f"--- Page Generation Pipeline Finished. Processed {processed_count} new pages. ---"
    )
