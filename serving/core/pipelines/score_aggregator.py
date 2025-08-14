# serving/core/pipelines/score_aggregator.py
import logging
import pandas as pd
from datetime import datetime
import re
from .. import config, gcs, bq

def _gather_analysis_files() -> dict:
    # ... (logic from original aggregator.py)
    ticker_data = {}
    score_regex = re.compile(r'"score"\s*:\s*([0-9.]+)')
    analysis_regex = re.compile(r'"analysis"\s*:\s*"(.*?)"\s*}', re.DOTALL)
    summary_regex = re.compile(r'"summary"\s*:\s*"(.*?)"\s*}', re.DOTALL)

    for analysis_type, prefix in config.ANALYSIS_PREFIXES.items():
        blobs = gcs.list_blobs_with_content(config.GCS_BUCKET_NAME, prefix)
        for blob_name, content in blobs.items():
            try:
                ticker = blob_name.split('/')[-1].split('_')[0]
                if ticker not in ticker_data:
                    ticker_data[ticker] = {"scores": {}, "texts": {}}
                if analysis_type == "business_summary":
                    match = summary_regex.search(content)
                    if match:
                        summary_text = match.group(1).replace('\\"', '"')
                        ticker_data[ticker]["texts"]["About"] = f"## About\n\n{summary_text}"
                else:
                    score_match = score_regex.search(content)
                    analysis_match = analysis_regex.search(content)
                    if score_match and analysis_match:
                        score = float(score_match.group(1))
                        analysis_text = analysis_match.group(1).replace('\\"', '"')
                        ticker_data[ticker]["scores"][f"{analysis_type}_score"] = score
                        title = analysis_type.replace('_', ' ').title()
                        ticker_data[ticker]["texts"][title] = f"## {title} Analysis\n\n{analysis_text}"
            except Exception as e:
                logging.error(f"Error processing {blob_name}: {e}")
    return ticker_data

def _process_and_score_data(ticker_data: dict) -> pd.DataFrame:
    # ... (logic from original aggregator.py)
    if not ticker_data: return pd.DataFrame()
    all_records = []
    for ticker, data in ticker_data.items():
        record = {"ticker": ticker}
        record.update(data.get("scores", {}))
        texts = data.get("texts", {})
        sorted_texts = []
        if "About" in texts:
            sorted_texts.append(texts.pop("About"))
        sorted_texts.extend(texts.values())
        record["aggregated_text"] = "\n\n---\n\n".join(sorted_texts)
        all_records.append(record)
    df = pd.DataFrame(all_records)
    df["run_date"] = datetime.now().date()
    score_cols = list(config.SCORE_WEIGHTS.keys())
    for col in score_cols:
        if col not in df.columns: df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors='coerce')
    complete_mask = df[score_cols].notna().all(axis=1)
    df["weighted_score"] = pd.NA
    if complete_mask.any():
        df_complete = df[complete_mask].copy()
        for col in score_cols:
            min_val, max_val = df_complete[col].min(), df_complete[col].max()
            df.loc[complete_mask, f"norm_{col}"] = (df_complete[col] - min_val) / (max_val - min_val) if (max_val - min_val) > 0 else 0.5
        norm_cols = [f"norm_{col}" for col in score_cols]
        weighted_sum = sum(df.loc[complete_mask, norm_col] * config.SCORE_WEIGHTS[col] for col, norm_col in zip(score_cols, norm_cols))
        df.loc[complete_mask, "weighted_score"] = weighted_sum
    final_cols = ['ticker', 'run_date', 'weighted_score', 'aggregated_text'] + score_cols
    df = df.reindex(columns=final_cols).drop(columns=[f"norm_{col}" for col in score_cols if f"norm_{col}" in df.columns])
    return df

def run_pipeline():
    """Main pipeline for score aggregation."""
    logging.info("--- Starting Score Aggregation Pipeline ---")
    ticker_data = _gather_analysis_files()
    if not ticker_data:
        logging.info("No analysis files found to aggregate.")
        return
    final_df = _process_and_score_data(ticker_data)
    if final_df.empty:
        logging.info("No data to load after processing.")
        return
    bq.load_df_to_bq(final_df, config.SCORES_TABLE_ID, config.SOURCE_PROJECT_ID)
    logging.info("--- Score Aggregation Pipeline Finished ---")