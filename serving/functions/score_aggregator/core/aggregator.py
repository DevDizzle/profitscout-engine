import logging
import json
import pandas as pd
from datetime import datetime
from . import config, gcs, bq
import re

def gather_analysis_files() -> dict:
    """
    Gathers all analysis files using a simplified, direct extraction method.
    """
    ticker_data = {}
    json_extractor = re.compile(r'\{.*\}', re.DOTALL)

    for analysis_type, prefix in config.ANALYSIS_PREFIXES.items():
        blobs = gcs.list_blobs_with_content(config.GCS_BUCKET_NAME, prefix)
        for blob_name, content in blobs.items():
            try:
                if not content or content.isspace():
                    continue
                
                match = json_extractor.search(content)
                if not match:
                    logging.error(f"UNRECOVERABLE: No JSON block found in {blob_name}.")
                    continue

                data = json.loads(match.group(0))

                if "score" in data and "analysis" in data:
                    ticker = blob_name.split('/')[-1].split('_')[0]
                    if ticker not in ticker_data:
                        ticker_data[ticker] = {"scores": {}, "texts": {}}
                    
                    ticker_data[ticker]["scores"][f"{analysis_type}_score"] = data["score"]
                    ticker_data[ticker]["texts"][analysis_type] = f"## {analysis_type.upper()} Analysis\n\n{data['analysis']}"

            except Exception:
                logging.error(f"UNRECOVERABLE PARSE ERROR for file {blob_name}.")
                
    return ticker_data

def process_and_score_data(ticker_data: dict) -> pd.DataFrame:
    """
    Creates a DataFrame containing only tickers with a complete set of scores,
    then normalizes and weights them. It does not impute any missing data.
    """
    # --- STRICT FILTERING LOGIC ---
    # 1. Define what a complete set of scores is.
    required_scores = set(config.SCORE_WEIGHTS.keys())
    
    complete_records = []
    for ticker, data in ticker_data.items():
        # 2. Check if the ticker has all the required scores.
        if required_scores.issubset(data.get("scores", {}).keys()):
            record = {"ticker": ticker}
            record.update(data["scores"])
            record["aggregated_text"] = "\n\n---\n\n".join(data["texts"].values())
            complete_records.append(record)
        else:
            logging.warning(f"Excluding ticker '{ticker}' due to incomplete analysis data.")
            
    if not complete_records:
        return pd.DataFrame()
        
    df = pd.DataFrame(complete_records)
    df["run_date"] = datetime.now().date()

    # --- No Imputation - This section now only works on complete data ---
    score_cols = list(config.SCORE_WEIGHTS.keys())
    for col in score_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        min_val = df[col].min()
        max_val = df[col].max()
        if max_val - min_val > 0:
            df[f"norm_{col}"] = (df[col] - min_val) / (max_val - min_val)
        else:
            df[f"norm_{col}"] = 0.5

    weighted_sum = sum(df[f"norm_{col}"] * config.SCORE_WEIGHTS[col] for col in score_cols)
    df["weighted_score"] = weighted_sum
    
    final_cols = ['ticker', 'run_date', 'weighted_score', 'aggregated_text'] + score_cols
    return df[final_cols]

def load_data_to_bq(final_df: pd.DataFrame):
    """
    A wrapper function to load the final DataFrame to BigQuery.
    """
    if final_df.empty:
        logging.warning("Final DataFrame is empty after filtering. No data will be loaded.")
        return
    bq.load_df_to_bq(final_df, config.SCORES_TABLE_ID)