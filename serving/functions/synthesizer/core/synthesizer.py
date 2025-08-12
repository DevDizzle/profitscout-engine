import logging
import json
import pandas as pd
from datetime import datetime
from . import config, gcs, bq
from .clients import vertex_ai

def gather_analysis_files() -> dict:
    """Gathers all analysis JSON files from GCS."""
    ticker_data = {}
    for analysis_type, prefix in config.ANALYSIS_PREFIXES.items():
        blobs = gcs.list_blobs_with_content(config.GCS_BUCKET_NAME, prefix)
        for blob_name, content in blobs.items():
            try:
                ticker = blob_name.split('/')[-1].split('_')[0]
                if ticker not in ticker_data:
                    ticker_data[ticker] = {"scores": {}, "texts": {}}
                data = json.loads(content)
                ticker_data[ticker]["scores"][analysis_type] = data.get("score")
                ticker_data[ticker]["texts"][analysis_type] = data.get("analysis")
            except (json.JSONDecodeError, IndexError) as e:
                logging.warning(f"Could not process file {blob_name}: {e}")
    logging.info(f"Gathered analysis data for {len(ticker_data)} tickers.")
    return ticker_data

def create_scores_dataframe(ticker_data: dict) -> pd.DataFrame:
    """Creates a pandas DataFrame from the collected scores."""
    records = []
    for ticker, data in ticker_data.items():
        record = {"ticker": ticker, "run_date": datetime.now().date()}
        for analysis_type, score in data["scores"].items():
            record[f"{analysis_type}_score"] = score
        records.append(record)
    return pd.DataFrame(records)

def load_scores_to_bq(scores_df: pd.DataFrame):
    """Loads the scores DataFrame to the BigQuery scores table."""
    bq.load_df_to_bq(scores_df, config.SCORES_TABLE_ID)

def get_weighted_scores() -> pd.DataFrame:
    """Executes the SQL to calculate weighted scores and returns the result."""
    return bq.run_synthesis_query()

def generate_and_save_recommendations(weighted_scores_df: pd.DataFrame, ticker_data: dict):
    """
    Generates the final recommendation for each ticker and saves it to GCS.
    """
    processed_count = 0
    for _, row in weighted_scores_df.iterrows():
        ticker = row['ticker']
        weighted_score = row['weighted_score']
        
        if weighted_score > 0.7: recommendation = "BUY"
        elif weighted_score >= 0.5: recommendation = "HOLD"
        else: recommendation = "SELL"
            
        combined_text = "\n\n---\n\n".join(
            f"## {analysis_type.upper()} Analysis\n\n{text}"
            for analysis_type, text in ticker_data.get(ticker, {}).get("texts", {}).items()
        )
        
        prompt = f"""You are the "ProfitScout Synthesizer", an expert financial analyst AI. Your task is to provide a final investment recommendation based on a pre-calculated weighted score and a collection of analyses.

        **Calculated Recommendation:** {recommendation} (based on a weighted score of {weighted_score:.2f})

        **Your Task:**
        1. Write a concise, confident, and cohesive summary that **justifies the calculated recommendation**.
        2. Seamlessly weave together the key findings from the different analysis sections provided below.
        3. Do **NOT** calculate your own score or recommendation. Your job is to explain the one provided.
        4. Structure your output into two or three clear paragraphs.

        ---
        **Combined Analysis Report:**
        {combined_text}
        ---

        **Final Synthesis:**"""
        
        try:
            final_summary = vertex_ai.generate(prompt)
        except Exception as e:
            logging.error(f"LLM synthesis generation failed for {ticker}: {e}", exc_info=True)
            final_summary = f"Based on a comprehensive data analysis, the model recommends a '{recommendation}' rating with a weighted score of {weighted_score:.2f}. A detailed narrative could not be generated."
        
        # Prepare the final output object
        result = {
            "ticker": ticker,
            "recommendation": recommendation,
            "weighted_score": weighted_score,
            "final_summary": final_summary,
            "raw_analysis_text": combined_text,
            "last_updated": datetime.now().isoformat()
        }
        
        # Save the result to a unique file in GCS
        output_blob_name = f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation.json"
        gcs.write_json_to_gcs(config.GCS_BUCKET_NAME, output_blob_name, result)
        
        logging.info(f"Generated and saved recommendation for {ticker}: {recommendation} ({weighted_score:.2f})")
        processed_count += 1
        
    return processed_count