# serving/functions/recommendation_generator/core/recommender.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from . import config, gcs, bq
from .clients import vertex_ai

# One-shot example for consistent output format
_EXAMPLE_OUTPUT = """{
  "recommendation": "HOLD",
  "confidence": "MEDIUM",
  "analysis": "The comprehensive analysis suggests a neutral to slightly positive outlook. The weighted score of 0.58 indicates that while there are positive catalysts, such as strong technical momentum and a recent earnings beat, they are tempered by significant headwinds identified in the MD&A, including rising input costs and increased competition. The fundamental analysis points to a fair valuation, but the news sentiment is mixed. Given the conflicting signals, the most prudent approach is to hold the current position. A significant positive development, such as a major contract win or a substantial improvement in forward guidance, would be required to upgrade the recommendation to a BUY."
}"""

def process_ticker_data(ticker_data: dict):
    """
    Generates and uploads a recommendation for a single ticker.
    """
    ticker = ticker_data.get("ticker")
    weighted_score = ticker_data.get("weighted_score")
    aggregated_text = ticker_data.get("aggregated_text")

    if not all([ticker, weighted_score, aggregated_text]):
        logging.warning(f"Skipping recommendation for a record due to missing data: {ticker_data}")
        return None

    recommendation_blob_path = f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation.json"
    logging.info(f"[{ticker}] Generating recommendation.")

    # === PROMPT PLACEHOLDER ===
    # As requested, here is the placeholder for your prompt.
    # You can refine this to better suit your needs.
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
        # Generate the recommendation using the Vertex AI client
        recommendation_json = vertex_ai.generate(prompt)

        # Save the recommendation to GCS
        gcs.write_text(
            config.GCS_BUCKET_NAME,
            recommendation_blob_path,
            recommendation_json,
            "application/json"
        )
        # Clean up any older versions of the recommendation file for this ticker
        gcs.cleanup_old_files(config.GCS_BUCKET_NAME, config.RECOMMENDATION_PREFIX, ticker, recommendation_blob_path)

        logging.info(f"[{ticker}] Successfully generated and saved recommendation.")
        return recommendation_blob_path

    except Exception as e:
        logging.error(f"[{ticker}] Failed to generate recommendation: {e}", exc_info=True)
        return None

def run_pipeline():
    """
    Orchestrates the entire recommendation generation pipeline.
    """
    logging.info("--- Starting Recommendation Generation Pipeline ---")

    # 1. Fetch the latest analysis scores from BigQuery
    scores_data = bq.get_analysis_scores()
    if not scores_data:
        logging.warning("No analysis scores found in BigQuery. Exiting pipeline.")
        return

    # 2. Process each ticker in parallel
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_ticker_data, item) for item in scores_data]
        count = sum(1 for future in as_completed(futures) if future.result())

    logging.info(f"--- Recommendation Pipeline Finished. Processed {count} tickers. ---")