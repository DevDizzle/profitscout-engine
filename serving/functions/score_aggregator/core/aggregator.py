import logging
import json
import pandas as pd
from datetime import datetime
from . import config, gcs, bq
import re

def gather_analysis_files() -> dict:
    """
    Gathers all analysis files, including the new business summary,
    using robust regex to extract data even from malformed JSON.
    """
    ticker_data = {}
    logging.info("Starting analysis file gathering...")

    score_regex = re.compile(r'"score"\s*:\s*([0-9.]+)')
    analysis_regex = re.compile(r'"analysis"\s*:\s*"(.*?)"\s*}', re.DOTALL)
    summary_regex = re.compile(r'"summary"\s*:\s*"(.*?)"\s*}', re.DOTALL) # For business summary

    for analysis_type, prefix in config.ANALYSIS_PREFIXES.items():
        blobs = gcs.list_blobs_with_content(config.GCS_BUCKET_NAME, prefix)

        for blob_name, content in blobs.items():
            try:
                ticker = blob_name.split('/')[-1].split('_')[0]
                if ticker not in ticker_data:
                    ticker_data[ticker] = {"scores": {}, "texts": {}}

                # --- MODIFIED LOGIC ---
                # Handle the new business summary file
                if analysis_type == "business_summary":
                    match = summary_regex.search(content)
                    if match:
                        summary_text = match.group(1).replace('\\"', '"')
                        # Add to the 'texts' dict with a specific title
                        ticker_data[ticker]["texts"]["About"] = f"## About\n\n{summary_text}"
                    else:
                        logging.warning(f"Could not extract summary from {blob_name}")
                    continue # Move to the next file

                # Original logic for all other analysis files
                score_match = score_regex.search(content)
                analysis_match = analysis_regex.search(content)

                if score_match and analysis_match:
                    score = float(score_match.group(1))
                    analysis_text = analysis_match.group(1).replace('\\"', '"')

                    ticker_data[ticker]["scores"][f"{analysis_type}_score"] = score
                    title = analysis_type.replace('_', ' ').title()
                    ticker_data[ticker]["texts"][title] = f"## {title} Analysis\n\n{analysis_text}"
                else:
                    logging.error(f"Could not extract score/analysis from {blob_name}. Skipping file.")

            except Exception as e:
                logging.error(f"An unexpected error occurred while processing {blob_name}: {e}")

    logging.info(f"Finished gathering files. Found data for {len(ticker_data)} unique tickers.")
    return ticker_data

def process_and_score_data(ticker_data: dict) -> pd.DataFrame:
    """
    Processes the gathered data, ensuring the "About" summary is first,
    and then calculates the weighted score for complete records.
    """
    if not ticker_data:
        return pd.DataFrame()

    all_records = []
    for ticker, data in ticker_data.items():
        record = {"ticker": ticker}
        record.update(data.get("scores", {}))

        # --- Ensure "About" comes first ---
        texts = data.get("texts", {})
        sorted_texts = []
        if "About" in texts:
            sorted_texts.append(texts.pop("About"))
        # Add the rest of the texts
        sorted_texts.extend(texts.values())

        record["aggregated_text"] = "\n\n---\n\n".join(sorted_texts)
        all_records.append(record)

    df = pd.DataFrame(all_records)
    df["run_date"] = datetime.now().date()

    # --- Scoring logic remains the same ---
    score_cols = list(config.SCORE_WEIGHTS.keys())

    for col in score_cols:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors='coerce')

    complete_mask = df[score_cols].notna().all(axis=1)
    df["weighted_score"] = pd.NA

    if complete_mask.any():
        df_complete = df[complete_mask].copy()
        for col in score_cols:
            min_val, max_val = df_complete[col].min(), df_complete[col].max()
            if (max_val - min_val) > 0:
                df.loc[complete_mask, f"norm_{col}"] = (df_complete[col] - min_val) / (max_val - min_val)
            else:
                df.loc[complete_mask, f"norm_{col}"] = 0.5

        norm_cols = [f"norm_{col}" for col in score_cols]
        weighted_sum = sum(df.loc[complete_mask, norm_col] * config.SCORE_WEIGHTS[col] for col, norm_col in zip(score_cols, norm_cols))
        df.loc[complete_mask, "weighted_score"] = weighted_sum

    logging.info(f"Processed {len(df)} tickers. Found {complete_mask.sum()} tickers with complete scores for weighting.")

    final_cols = ['ticker', 'run_date', 'weighted_score', 'aggregated_text'] + score_cols
    df = df.reindex(columns=final_cols)
    norm_cols_to_drop = [f"norm_{col}" for col in score_cols if f"norm_{col}" in df.columns]
    df.drop(columns=norm_cols_to_drop, inplace=True)

    return df


def load_data_to_bq(final_df: pd.DataFrame):
    """Loads the final DataFrame into the specified BigQuery table."""
    if final_df.empty:
        logging.warning("Final DataFrame is empty. No data will be loaded to BigQuery.")
        return
    logging.info(f"Loading {len(final_df)} rows into BigQuery...")
    bq.load_df_to_bq(final_df, config.SCORES_TABLE_ID)