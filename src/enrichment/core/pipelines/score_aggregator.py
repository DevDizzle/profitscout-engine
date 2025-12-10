import logging
import pandas as pd
from datetime import datetime
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from .. import config, gcs
from google.cloud import bigquery, storage

# Use standard logging for Cloud Functions
logging.basicConfig(level=logging.INFO)

def _read_and_parse_blob(blob_name: str, analysis_type: str, storage_client: storage.Client) -> tuple | None:
    """
    Helper function to read and parse a single blob.
    Crucial Fix: Accepts 'storage_client' to reuse the existing connection pool.
    """
    try:
        # Pass the shared client to gcs.read_blob to avoid opening a new connection
        content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name, client=storage_client)
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
        logging.warning(f"Worker could not process blob {blob_name}: {e}")
        return None

def _gather_analysis_data() -> dict:
    """
    Gathers both scores and analysis text using a Single Shared Client.
    """
    ticker_data = {}
    all_prefixes = config.ANALYSIS_PREFIXES

    # --- FIX: Initialize Client ONCE ---
    # This single client manages the connection pool for all threads.
    storage_client = storage.Client(project=config.PROJECT_ID)

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS * 4) as executor:
        future_to_blob = {}
        logging.info("--> Starting to list and submit files for each analysis type...")
        
        for analysis_type, prefix in all_prefixes.items():
            # Reuse client for listing as well
            blobs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix, client=storage_client)
            
            for blob_name in blobs:
                # --- FIX: Pass shared client to worker ---
                future = executor.submit(_read_and_parse_blob, blob_name, analysis_type, storage_client)
                future_to_blob[future] = blob_name

        processed_count = 0
        total_futures = len(future_to_blob)
        logging.info(f"--> Submitted {total_futures} tasks. Processing results...")

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
                logging.error(f"TIMEOUT: Worker for {blob_name_for_log} timed out.")
            except Exception as e:
                logging.error(f"ERROR: Worker for {blob_name_for_log} failed: {e}")

            processed_count += 1
            if processed_count % 500 == 0:
                logging.info(f"    ..... Progress: {processed_count}/{total_futures} files...")

    return ticker_data

def _calculate_regime_weighted_score(row: pd.Series) -> float:
    """
    Calculates weighted score using Dynamic Regime Logic.
    """
    # 1. Determine Regime (News Score deviation from 0.5)
    news_val = row.get("news_score", 0.5)
    
    # 0.70+ is Bullish Catalyst, 0.30- is Bearish Catalyst
    is_event_regime = (news_val >= 0.70) or (news_val <= 0.30)
    
    # 2. Select Weight Profile
    weights = config.SCORE_WEIGHTS_EVENT if is_event_regime else config.SCORE_WEIGHTS_QUIET
    
    # 3. Calculate Score
    final_score = 0.0
    for col, weight in weights.items():
        val = row.get(col, 0.5)
        try:
            val = float(val)
        except (ValueError, TypeError):
            val = 0.5
        final_score += val * weight
        
    return final_score

def _process_and_score_data(ticker_data: dict) -> pd.DataFrame:
    if not ticker_data:
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(ticker_data, orient='index').reset_index().rename(columns={'index': 'ticker'})
    df["run_date"] = datetime.now().date()

    # Ensure all score columns exist
    for col in config.SCORE_COLS:
        if col not in df.columns:
            df[col] = 0.5
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.5)

    # --- DYNAMIC SCORING ---
    df["weighted_score"] = df.apply(_calculate_regime_weighted_score, axis=1)

    # Calculate percentile rank
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

    final_cols = ['ticker', 'run_date', 'weighted_score', 'score_percentile', 'aggregated_text'] + config.SCORE_COLS
    return df.reindex(columns=final_cols)

def run_pipeline():
    logging.info("--- Starting Score Aggregation Pipeline ---")
    client = bigquery.Client(project=config.PROJECT_ID)

    logging.info("STEP 1: Starting to gather analysis data from GCS...")
    ticker_scores = _gather_analysis_data()
    if not ticker_scores:
        logging.warning("No ticker data was gathered from GCS. Exiting.")
        return
    logging.info(f"STEP 1 COMPLETE: Gathered data for {len(ticker_scores)} tickers.")

    logging.info("STEP 2: Starting to process and score data with pandas...")
    final_df = _process_and_score_data(ticker_scores)
    if final_df.empty:
        logging.warning("DataFrame is empty after processing. Exiting.")
        return
    logging.info(f"STEP 2 COMPLETE: Processed data into a DataFrame with shape {final_df.shape}.")

    logging.info("STEP 3: Starting to load DataFrame to BigQuery...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(final_df, config.SCORES_TABLE_ID, job_config=job_config)
    job.result()
    logging.info(f"STEP 3 COMPLETE: Loaded {job.output_rows} rows into BigQuery table: {config.SCORES_TABLE_ID}")

    logging.info("--- Score Aggregation Pipeline Finished ---")