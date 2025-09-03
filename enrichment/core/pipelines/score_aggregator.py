# enrichment/core/pipelines/score_aggregator.py
import logging
import pandas as pd
from datetime import datetime
import re
from .. import config, gcs
from google.cloud import bigquery

def _gather_analysis_scores() -> dict:
    """
    Gathers only the scores from all analysis files in GCS.
    """
    ticker_data = {}
    score_regex = re.compile(r'"score"\s*:\s*([0-9.]+)')

    # Filter out business_summary since it has no score
    analysis_prefixes = {k: v for k, v in config.ANALYSIS_PREFIXES.items() if k != "business_summary"}

    for analysis_type, prefix in analysis_prefixes.items():
        blobs = gcs.list_blobs_with_content(config.GCS_BUCKET_NAME, prefix)
        for blob_name, content in blobs.items():
            try:
                ticker = blob_name.split('/')[-1].split('_')[0]
                if ticker not in ticker_data:
                    ticker_data[ticker] = {}
                
                score_match = score_regex.search(content)
                if score_match:
                    score = float(score_match.group(1))
                    ticker_data[ticker][f"{analysis_type}_score"] = score
            except Exception as e:
                logging.error(f"Error processing score from {blob_name}: {e}")
    return ticker_data

def _process_and_score_data(ticker_data: dict) -> pd.DataFrame:
    """
    Processes the gathered scores, calculates the weighted score, and returns a DataFrame.
    """
    if not ticker_data: 
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(ticker_data, orient='index').reset_index().rename(columns={'index': 'ticker'})
    df["run_date"] = datetime.now().date()
    
    score_cols = list(config.SCORE_WEIGHTS.keys())

    for col in score_cols:
        if col not in df.columns: 
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df["weighted_score"] = pd.NA
    complete_mask = df[score_cols].notna().all(axis=1)

    if complete_mask.any():
        df_complete = df[complete_mask].copy()
        
        # Normalize scores
        for col in score_cols:
            min_val, max_val = df_complete[col].min(), df_complete[col].max()
            df_complete[f"norm_{col}"] = (df_complete[col] - min_val) / (max_val - min_val) if (max_val - min_val) > 0 else 0.5
        
        norm_cols = [f"norm_{col}" for col in score_cols]
        
        # Calculate weighted score
        df_complete["weighted_score"] = sum(df_complete[norm_col] * config.SCORE_WEIGHTS[col] for col, norm_col in zip(score_cols, norm_cols))
        
        # Update the original DataFrame
        df.update(df_complete[["weighted_score"]])

    # Define final columns without aggregated_text
    final_cols = ['ticker', 'run_date', 'weighted_score'] + score_cols
    return df.reindex(columns=final_cols)


def run_pipeline():
    """Main pipeline for score aggregation."""
    logging.info("--- Starting Score Aggregation Pipeline (Scores Only) ---")
    client = bigquery.Client(project=config.PROJECT_ID)
    ticker_scores = _gather_analysis_scores()

    if not ticker_scores:
        logging.info("No analysis files with scores found to aggregate.")
        return

    final_df = _process_and_score_data(ticker_scores)
    if final_df.empty:
        logging.info("No data to load after processing.")
        return
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = client.load_table_from_dataframe(final_df, config.SCORES_TABLE_ID, job_config=job_config)
    job.result()
    logging.info(f"Loaded {job.output_rows} rows into BigQuery table: {config.SCORES_TABLE_ID}")
    
    logging.info("--- Score Aggregation Pipeline Finished ---")