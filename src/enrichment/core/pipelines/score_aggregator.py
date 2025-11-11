import logging
import pandas as pd
from datetime import datetime
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from .. import config, gcs
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)


def _read_and_parse_blob(blob_name: str, analysis_type: str) -> tuple[str | None, dict] | None:
    """Helper function to read and parse a single blob in a thread pool."""
    try:
        content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
        if not content:
            return None

        if analysis_type == "macro_thesis":
            try:
                payload = json.loads(content)
            except json.JSONDecodeError:
                payload = {}

            macro_trend = (payload.get("macro_trend") or "").strip()
            anti_thesis = (payload.get("anti_thesis") or "").strip()
            generated_at = payload.get("generated_at")

            return None, {
                "macro_trend": macro_trend,
                "anti_thesis": anti_thesis,
                "macro_generated_at": generated_at,
                "macro_blob_name": blob_name,
            }

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

        return ticker, parsed_data
    except Exception as e:
        print(f"WARNING: Worker could not process blob {blob_name}: {e}")
        return None

def _gather_analysis_data() -> tuple[dict, dict | None]:
    """
    Gathers both scores and analysis text from all analysis files in GCS in parallel.
    """
    ticker_data = {}
    macro_context: dict | None = None
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
                future_to_blob[future] = (analysis_type, blob_name)

        processed_count = 0
        total_futures = len(future_to_blob)
        print(f"--> All {total_futures} read tasks submitted. Now processing results as they complete...")

        for future in as_completed(future_to_blob):
            analysis_type_for_log, blob_name_for_log = future_to_blob[future]
            try:
                result = future.result(timeout=15)
                if result:
                    ticker, data = result
                    if analysis_type_for_log == "macro_thesis":
                        if not data:
                            continue
                        if macro_context is None:
                            macro_context = data
                        else:
                            new_generated_at = data.get("macro_generated_at")
                            current_generated_at = macro_context.get("macro_generated_at")
                            if new_generated_at and current_generated_at:
                                if new_generated_at > current_generated_at:
                                    macro_context = data
                            elif blob_name_for_log > macro_context.get("macro_blob_name", ""):
                                macro_context = data
                    else:
                        if ticker not in ticker_data:
                            ticker_data[ticker] = {}
                        ticker_data[ticker].update(data)
            except TimeoutError:
                print(f"HARD TIMEOUT: Worker for blob {blob_name_for_log} took longer than 15 seconds. Skipping.")
            except Exception as e:
                print(f"ERROR: Worker for blob {blob_name_for_log} failed with an unexpected error: {e}")

            processed_count += 1
            if processed_count % 500 == 0:
                print(f"    ..... Progress: Processed {processed_count} of {total_futures} files...")

    print(f"--> Finished processing all {total_futures} files.")
    return ticker_data, macro_context

def _process_and_score_data(ticker_data: dict, macro_context: dict | None) -> pd.DataFrame:
    """
    Processes the gathered data, calculates scores and percentile, aggregates text, and returns a DataFrame.
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

    # Calculate percentile rank for non-null weighted scores
    df['score_percentile'] = df['weighted_score'].rank(pct=True)

    macro_trend = ""
    anti_thesis = ""
    macro_generated_at = None
    if macro_context:
        macro_trend = macro_context.get("macro_trend", "") or ""
        anti_thesis = macro_context.get("anti_thesis", "") or ""
        macro_generated_at = macro_context.get("macro_generated_at")

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

        if macro_trend or anti_thesis:
            macro_section_lines = ["## Macro Thesis"]
            if macro_trend:
                macro_section_lines.append(f"**Trend:** {macro_trend}")
            if anti_thesis:
                macro_section_lines.append(f"**Anti-Thesis:** {anti_thesis}")
            text_parts.append("\n\n".join(macro_section_lines))

        return "\n\n---\n\n".join(text_parts)

    df["aggregated_text"] = df.apply(aggregate_text, axis=1)

    if macro_trend:
        df["macro_trend"] = macro_trend
    else:
        df["macro_trend"] = pd.NA

    if anti_thesis:
        df["anti_thesis"] = anti_thesis
    else:
        df["anti_thesis"] = pd.NA

    if macro_generated_at:
        df["macro_generated_at"] = macro_generated_at
    else:
        df["macro_generated_at"] = pd.NA

    # Add score_percentile to the final columns
    final_cols = ['ticker', 'run_date', 'weighted_score', 'score_percentile', 'aggregated_text', 'macro_trend', 'anti_thesis', 'macro_generated_at'] + score_cols
    return df.reindex(columns=final_cols)


def run_pipeline():
    """Main pipeline for score aggregation."""
    print("--- Starting Score Aggregation Pipeline ---")
    client = bigquery.Client(project=config.PROJECT_ID)

    print("STEP 1: Starting to gather analysis data from GCS...")
    ticker_scores, macro_context = _gather_analysis_data()
    if not ticker_scores:
        print("WARNING: No ticker data was gathered from GCS. Exiting.")
        return
    print(f"STEP 1 COMPLETE: Gathered data for {len(ticker_scores)} tickers.")

    print("STEP 2: Starting to process and score data with pandas...")
    final_df = _process_and_score_data(ticker_scores, macro_context)
    if final_df.empty:
        print("WARNING: DataFrame is empty after processing. Exiting.")
        return
    print(f"STEP 2 COMPLETE: Processed data into a DataFrame with shape {final_df.shape}.")

    print("STEP 3: Starting to load DataFrame to BigQuery...")
    # --- THIS IS THE FIX ---
    # Removed schema_update_options as it's incompatible with WRITE_TRUNCATE
    # BigQuery will infer the schema (including the new column) automatically.
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    # --- END FIX ---
    job = client.load_table_from_dataframe(final_df, config.SCORES_TABLE_ID, job_config=job_config)
    job.result()
    print(f"STEP 3 COMPLETE: Loaded {job.output_rows} rows into BigQuery table: {config.SCORES_TABLE_ID}")

    print("--- Score Aggregation Pipeline Finished ---")