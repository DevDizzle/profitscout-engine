# serving/core/pipelines/recommendation_generator.py
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from .. import config, gcs
from ..clients import vertex_ai

_EXAMPLE_OUTPUT = """{
  "recommendation": "HOLD",
  "confidence": "MEDIUM",
  "analysis": "The comprehensive analysis suggests a neutral to slightly positive outlook. The weighted score of 0.58 indicates that while there are positive catalysts, such as strong technical momentum and a recent earnings beat, they are tempered by significant headwinds identified in the MD&A, including rising input costs and increased competition. The fundamental analysis points to a fair valuation, but the news sentiment is mixed. Given the conflicting signals, the most prudent approach is to hold the current position."
}"""

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
    """Generates and uploads a recommendation for a single ticker."""
    ticker = ticker_data.get("ticker")
    weighted_score = ticker_data.get("weighted_score")
    aggregated_text = ticker_data.get("aggregated_text")

    if not all([ticker, weighted_score, aggregated_text]):
        return None

    blob_path = f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation.json"
    logging.info(f"[{ticker}] Generating recommendation.")

    prompt = r"""
You are an expert financial analyst providing a final investment recommendation.
Synthesize the provided `aggregated_text` and the `weighted_score` into a clear, actionable recommendation.

### Instructions
1.  **Recommendation**: Your final recommendation MUST be one of three values: "BUY", "SELL", or "HOLD".
2.  **Confidence**: Your confidence in this recommendation MUST be one of three values: "HIGH", "MEDIUM", or "LOW".
3.  **Analysis**: Provide a concise, one-paragraph summary of your reasoning. Weave together the key bullish and bearish points from the aggregated text to justify your recommendation and confidence level.

### Input Data
- **Weighted Score**: {weighted_score}
- **Aggregated Analysis Text**:
{aggregated_text}

### Example Output (for format only)
{example_output}

### Final Output (Return ONLY this JSON object)
    """.replace("{weighted_score}", str(weighted_score)) \
       .replace("{aggregated_text}", aggregated_text) \
       .replace("{example_output}", _EXAMPLE_OUTPUT)

    try:
        recommendation_json = vertex_ai.generate(prompt)
        gcs.write_text(config.GCS_BUCKET_NAME, blob_path, recommendation_json, "application/json")
        gcs.cleanup_old_files(config.GCS_BUCKET_NAME, config.RECOMMENDATION_PREFIX, ticker, blob_path)
        return blob_path
    except Exception as e:
        logging.error(f"[{ticker}] Failed to generate recommendation: {e}", exc_info=True)
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