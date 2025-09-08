import logging
import pandas as pd
from datetime import datetime
import re
import json
from .. import config, gcs
from google.cloud import bigquery

def _gather_analysis_data() -> dict:
    """
    Gathers both scores and analysis text from all analysis files in GCS.
    """
    ticker_data = {}
    score_regex = re.compile(r'"score"\s*:\s*([0-9.]+)')
    analysis_regex = re.compile(r'"analysis"\s*:\s*"(.*?)"', re.DOTALL)

    # Include business_summary now to fetch the 'about' text
    all_prefixes = config.ANALYSIS_PREFIXES

    for analysis_type, prefix in all_prefixes.items():
        blobs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix)
        for blob_name in blobs:
            try:
                content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
                if not content:
                    continue

                ticker = blob_name.split('/')[-1].split('_')[0]
                if ticker not in ticker_data:
                    ticker_data[ticker] = {}

                # For business summaries, just get the text
                if analysis_type == "business_summary":
                    summary_match = re.search(r'"summary"\s*:\s*"(.*?)"', content, re.DOTALL)
                    if summary_match:
                        ticker_data[ticker]["about"] = summary_match.group(1).replace('\\n', ' ').strip()
                    continue

                # For all others, get score and analysis
                score_match = score_regex.search(content)
                analysis_match = analysis_regex.search(content)

                if score_match:
                    score = float(score_match.group(1))
                    ticker_data[ticker][f"{analysis_type}_score"] = score
                
                if analysis_match:
                    analysis_text = analysis_match.group(1).replace('\\n', ' ').strip()
                    ticker_data[ticker][f"{analysis_type}_analysis"] = analysis_text

            except Exception as e:
                logging.error(f"Error processing data from {blob_name}: {e}")
    return ticker_data

def _process_and_score_data(ticker_data: dict) -> pd.DataFrame:
    """
    Processes the gathered data, calculates scores, aggregates text, and returns a DataFrame.
    """
    if not ticker_data:
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(ticker_data, orient='index').reset_index().rename(columns={'index': 'ticker'})
    df["run_date"] = datetime.now().date()

    score_cols = list(config.SCORE_WEIGHTS.keys())
    for col in score_cols:
        if col not in df.columns:
            df[col] = 0.5
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.5)

    df["weighted_score"] = pd.NA
    complete_mask = df[score_cols].notna().all(axis=1)

    if complete_mask.any():
        df_complete = df[complete_mask].copy()
        
        for col in score_cols:
            min_val, max_val = df_complete[col].min(), df_complete[col].max()
            df_complete[f"norm_{col}"] = (df_complete[col] - min_val) / (max_val - min_val) if (max_val - min_val) > 0 else 0.5
        
        norm_cols = [f"norm_{col}" for col in score_cols]
        df_complete["weighted_score"] = sum(df_complete[norm_col] * config.SCORE_WEIGHTS[col] for col, norm_col in zip(score_cols, norm_cols))
        
        df.update(df_complete[["weighted_score"]])

    # --- RE-IMPLEMENTED: Aggregate Text Section ---
    def aggregate_text(row):
        text_parts = []
        if pd.notna(row.get("about")):
            text_parts.append(f"## About\n\n{row['about']}")
        
        # Define the order and title for each analysis type
        analysis_order = {
            "news": "News", "technicals": "Technicals", "mda": "MD&A",
            "transcript": "Transcript", "financials": "Financials",
            "fundamentals": "Fundamentals"
        }
        for key, title in analysis_order.items():
            if pd.notna(row.get(f"{key}_analysis")):
                text_parts.append(f"## {title} Analysis\n\n{row[f'{key}_analysis']}")
        
        return "\n\n---\n\n".join(text_parts)

    df["aggregated_text"] = df.apply(aggregate_text, axis=1)
    # --- END of re-implementation ---

    final_cols = ['ticker', 'run_date', 'weighted_score', 'aggregated_text'] + score_cols
    return df.reindex(columns=final_cols)


def run_pipeline():
    """Main pipeline for score aggregation."""
    logging.info("--- Starting Score Aggregation Pipeline ---")
    client = bigquery.Client(project=config.PROJECT_ID)
    ticker_scores = _gather_analysis_data()

    if not ticker_scores:
        logging.info("No analysis files found to aggregate.")
        return

    final_df = _process_and_score_data(ticker_scores)
    if final_df.empty:
        logging.info("No data to load after processing.")
        return
    
    # Use WRITE_TRUNCATE to ensure the schema is updated correctly
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = client.load_table_from_dataframe(final_df, config.SCORES_TABLE_ID, job_config=job_config)
    job.result()
    logging.info(f"Loaded {job.output_rows} rows into BigQuery table: {config.SCORES_TABLE_ID}")
    
    logging.info("--- Score Aggregation Pipeline Finished ---")