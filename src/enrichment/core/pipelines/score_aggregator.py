import logging
import pandas as pd
from datetime import datetime
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from .. import config, gcs
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)


def _read_and_parse_blob(blob_name: str, analysis_type: str) -> tuple | None:
    """Helper function to read and parse a single blob in a thread pool."""
    try:
        content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
        if not content:
            return None

        ticker = blob_name.split('/')[-1].split('_')[0]
        parsed_data = {"ticker": ticker}

        if analysis_type == "business_summary":
            summary_match = re.search(r'"summary"\s*:\s*"(.*?)"', content, re.DOTALL)
            if summary_match:
                parsed_data["about"] = summary_match.group(1).replace('\\n', ' ').strip()
        else:
            score_match = re.search(r'"score"\s*:\s*([0-9.]+)', content)
            analysis_match = re.search(r'"analysis"\s*:\s*"(.*?)"', content, re.DOTALL)
            if score_match:
                parsed_data[f"{analysis_type}_score"] = float(score_match.group(1))
            if analysis_match:
                parsed_data[f"{analysis_type}_analysis"] = analysis_match.group(1).replace('\\n', ' ').strip()

        return parsed_data
    except Exception as e:
        print(f"WARNING: Worker could not process blob {blob_name}: {e}")
        return None

def _gather_analysis_data() -> dict:
    """
    Gathers both scores and analysis text from all analysis files in GCS in parallel.
    """
    ticker_data = {}
    all_prefixes = config.ANALYSIS_PREFIXES

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS * 4) as executor:
        future_to_blob = {}
        print("--> Starting to list and submit files for each analysis type...")
        for analysis_type, prefix in all_prefixes.items():
            print(f"    Lising blobs for analysis type: '{analysis_type}'...")
            blobs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix)
            print(f"    Found {len(blobs)} blobs for '{analysis_type}'. Submitting to workers...")
            for blob_name in blobs:
                future = executor.submit(_read_and_parse_blob, blob_name, analysis_type)
                future_to_blob[future] = blob_name

        processed_count = 0
        total_futures = len(future_to_blob)
        print(f"--> All {total_futures} read tasks submitted. Now processing results as they complete...")

        for future in as_completed(future_to_blob):
            blob_name_for_log = future_to_blob[future]
            try:
                result = future.result(timeout=15)
                if result:
                    ticker = result.pop("ticker")
                    if ticker not in ticker_data:
                        ticker_data[ticker] = {}
                    ticker_data[ticker].update(result)
            except TimeoutError:
                print(f"HARD TIMEOUT: Worker for blob {blob_name_for_log} took longer than 15 seconds. Skipping.")
            except Exception as e:
                print(f"ERROR: Worker for blob {blob_name_for_log} failed with an unexpected error: {e}")

            processed_count += 1
            if processed_count % 500 == 0:
                print(f"    ..... Progress: Processed {processed_count} of {total_futures} files...")

    print(f"--> Finished processing all {total_futures} files.")
    return ticker_data

def _process_and_score_data(ticker_data: dict) -> pd.DataFrame:
    """
    Processes the gathered data, calculates absolute weighted scores, aggregates text, and returns a DataFrame.
    """
    if not ticker_data:
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(ticker_data, orient='index').reset_index().rename(columns={'index': 'ticker'})
    df["run_date"] = datetime.now().date()

    score_cols = list(config.SCORE_WEIGHTS.keys())
    for col in score_cols:
        # Fill missing scores with 0.5 (Neutral) so they don't drag down the average artificially
        if col not in df.columns:
            df[col] = 0.5
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.5)

    df["weighted_score"] = pd.NA
    
    # --- FIXED: Absolute Scoring ---
    # We calculate the weighted sum of the RAW scores (0.0 to 1.0).
    # We DO NOT normalize against the batch min/max.
    df["weighted_score"] = sum(
        df[col] * config.SCORE_WEIGHTS[col] for col in score_cols
    )

    # Calculate percentile rank for reference/filtering only, NOT for signal generation
    df['score_percentile'] = df['weighted_score'].rank(pct=True)

    def aggregate_text(row):
        text_parts = []
        if pd.notna(row.get("about")):
            text_parts.append(f"## About\n\n{row['about']}")

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

    # Add score_percentile to the final columns
    final_cols = ['ticker', 'run_date', 'weighted_score', 'score_percentile', 'aggregated_text'] + score_cols
    return df.reindex(columns=final_cols)


def run_pipeline():
    """Main pipeline for score aggregation."""
    print("--- Starting Score Aggregation Pipeline ---")
    client = bigquery.Client(project=config.PROJECT_ID)

    print("STEP 1: Starting to gather analysis data from GCS...")
    ticker_scores = _gather_analysis_data()
    if not ticker_scores:
        print("WARNING: No ticker data was gathered from GCS. Exiting.")
        return
    print(f"STEP 1 COMPLETE: Gathered data for {len(ticker_scores)} tickers.")

    print("STEP 2: Starting to process and score data with pandas...")
    final_df = _process_and_score_data(ticker_scores)
    if final_df.empty:
        print("WARNING: DataFrame is empty after processing. Exiting.")
        return
    print(f"STEP 2 COMPLETE: Processed data into a DataFrame with shape {final_df.shape}.")

    print("STEP 3: Starting to load DataFrame to BigQuery...")
    # Use WRITE_TRUNCATE to replace the daily snapshot
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(final_df, config.SCORES_TABLE_ID, job_config=job_config)
    job.result()
    print(f"STEP 3 COMPLETE: Loaded {job.output_rows} rows into BigQuery table: {config.SCORES_TABLE_ID}")

    print("--- Score Aggregation Pipeline Finished ---")