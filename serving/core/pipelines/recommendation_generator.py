# serving/core/pipelines/recommendation_generator.py
import logging
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from .. import config, gcs
from ..clients import vertex_ai

_EXAMPLE_OUTPUT = """
# Wolfspeed (WOLF)

## About
Wolfspeed, Inc. is a wide bandgap semiconductor company specializing in silicon carbide and gallium nitride materials and power devices, targeting applications in electric vehicles, renewable energy, motor drives, and telecommunications, with revenue from sales to a concentrated customer base.

## Recommendation
SELL

## Score
0.30925

## Analysis
The aggregated analysis indicates a predominantly bearish outlook for Wolfspeed (WOLF), justified by the low weighted score of 0.30925, driven by severe financial distress and operational challenges. Key points by section:

- **News**: Bankruptcy filings and high risks dominate, with stock surges seen as temporary amid investor aversion, suggesting moderate downward pressure in 1-2 weeks.
- **Technicals**: Mildly bearish trends, with price consolidation below key EMAs, negative momentum indicators, and potential further declines, despite slight OBV bullish divergence.
- **Management Discussion & Analysis**: Strongly bearish, highlighting revenue declines, gross margin collapse to -17.1%, widening losses, liquidity issues, and substantial doubt about going concern.
- **Earnings Call**: Moderately bullish long-term via restructuring, CHIPS Act funding, and EV design wins, but tempered by near-term revenue misses, soft guidance, and market weaknesses.
- **Financials**: Bearish trajectory with negative profits, cash burn, increasing debt to $6.52B, and weakening liquidity (cash down to $1.33B).
- **Fundamentals**: Bearish from persistent negative profitability and cash flows, high leverage and debt-to-equity, volatile liquidity, negative margins and interest coverage, inefficient asset utilization, and concerning valuations, despite strong current ratio.

Overall, overwhelming bearish signals outweigh limited bullish prospects, warranting a sell recommendation.

If you'd like more details, feel free to ask follow-up questions on any of the section titles, such as News or Earnings Call.
"""

_PROMPT_TEMPLATE = r"""
You are an expert financial analyst providing a final investment recommendation. Think step-by-step: First, determine the recommendation strictly based on the weighted_score thresholds. Second, extract and summarize key bullish and bearish points from each section of the aggregated_text. Third, weave them into a concise analysis with bullet points for sections. Finally, justify the recommendation and end with an encouragement for follow-up questions.

### Section Name Mapping
Use the following mapping for the bullet points in your analysis:
- **News**: "News Analysis" section
- **Technicals**: "Technicals Analysis" section
- **Management Discussion & Analysis**: "Mda Analysis" section
- **Earnings Call**: "Transcript Analysis" section
- **Financials**: "Financials Analysis" section
- **Fundamentals**: "Key Metrics Analysis" and "Ratios Analysis" sections combined

### Instructions
1.  **Recommendation**: MUST be one of: "BUY" (score > 0.68), "HOLD" (0.50 to 0.67 inclusive), "SELL" (< 0.50).
2.  **Score**: Display the weighted_score exactly as provided, without extra zeros (e.g., 0.30925).
3.  **Analysis**: Provide a brief intro sentence, then bullet points for key section summaries (use the shortened section titles from the mapping above as bold sub-bullets), and a concluding overall justification. Keep dense and concise for mobile users.
4.  **Format**: Output ONLY the Markdown below. Use # for company/ticker H1, ## for sections. End with the follow-up prompt.

### Input Data
- **Weighted Score**: {weighted_score}
- **Aggregated Analysis Text**:
{aggregated_text}

### Example Output
{example_output}
"""

def _get_analysis_scores() -> list[dict]:
    """Fetches the latest scored records from BigQuery."""
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    logging.info(f"Querying for analysis scores: {config.SCORES_TABLE_ID}")
    query = f"""
        SELECT ticker, weighted_score, aggregated_text
        FROM `{config.SCORES_TABLE_ID}`
        WHERE weighted_score IS NOT NULL AND aggregated_text IS NOT NULL
    """
    try:
        df = client.query(query).to_dataframe()
        logging.info(f"Successfully fetched {len(df)} records for recommendation.")
        return df.to_dict('records')
    except Exception as e:
        logging.critical(f"Failed to query for analysis scores: {e}", exc_info=True)
        return []

def _process_ticker(ticker_data: dict):
    """Generates and uploads both a JSON and a Markdown recommendation for a single ticker."""
    ticker = ticker_data.get("ticker")
    weighted_score = ticker_data.get("weighted_score")
    aggregated_text = ticker_data.get("aggregated_text")

    if not all([ticker, weighted_score, aggregated_text]):
        return None

    # Define paths for both output files
    json_blob_path = f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation.json"
    md_blob_path = f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation.md"

    logging.info(f"[{ticker}] Generating recommendation (JSON and Markdown).")

    # 1. Determine the recommendation programmatically for the JSON payload
    if weighted_score > 0.68:
        recommendation = "BUY"
    elif weighted_score >= 0.50:
        recommendation = "HOLD"
    else:
        recommendation = "SELL"

    # 2. Create the prompt to generate the detailed Markdown report
    prompt = _PROMPT_TEMPLATE.format(
        weighted_score=str(weighted_score),
        aggregated_text=aggregated_text,
        example_output=_EXAMPLE_OUTPUT,
    )

    try:
        # 3. Generate the rich Markdown analysis using the LLM
        recommendation_md = vertex_ai.generate(prompt)

        # 4. Create the simple, machine-readable JSON payload
        json_payload = {
            "recommendation": recommendation,
            "score": weighted_score,
            # Storing the full markdown in the JSON is a good practice for data consistency
            "analysis_markdown": recommendation_md
        }

        # 5. Upload both files to Google Cloud Storage
        gcs.write_text(config.GCS_BUCKET_NAME, json_blob_path, json.dumps(json_payload, indent=2), "application/json")
        gcs.write_text(config.GCS_BUCKET_NAME, md_blob_path, recommendation_md, "text/markdown")
        logging.info(f"[{ticker}] Successfully wrote both JSON and Markdown files.")

        # 6. Clean up old files for this ticker
        # (This assumes cleanup can handle multiple files; if not, this may need adjustment)
        gcs.cleanup_old_files(config.GCS_BUCKET_NAME, config.RECOMMENDATION_PREFIX, ticker, json_blob_path)
        gcs.cleanup_old_files(config.GCS_BUCKET_NAME, config.RECOMMENDATION_PREFIX, ticker, md_blob_path)

        return md_blob_path # Return a success indicator
    except Exception as e:
        logging.error(f"[{ticker}] Failed to generate or upload recommendation: {e}", exc_info=True)
        return None

def run_pipeline():
    """Orchestrates the recommendation generation pipeline."""
    logging.info("--- Starting Recommendation Generation Pipeline ---")
    scores_data = _get_analysis_scores()
    if not scores_data:
        logging.warning("No analysis scores found. Exiting pipeline.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS_RECOMMENDER) as executor:
        futures = [executor.submit(_process_ticker, item) for item in scores_data]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- Recommendation Pipeline Finished. Processed {count} tickers. ---")