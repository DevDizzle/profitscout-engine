# serving/core/pipelines/winners_dashboard_generator.py
import logging
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from .. import config, gcs, bq

# --- Configuration ---
RECOMMENDATION_PREFIX = "recommendations/"
SIGNALS_TABLE_ID = f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_analysis_signals"
ASSET_METADATA_TABLE_ID = f"{config.DESTINATION_PROJECT_ID}.{config.BIGQUERY_DATASET}.asset_metadata"
OUTPUT_TABLE_ID = f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.winners_dashboard"
DAILY_PREDICTIONS_TABLE_ID = f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.daily_predictions"

# --- Main Logic ---

def _get_all_stock_recommendations() -> pd.DataFrame:
    """
    Fetches ALL companion JSON files.
    REMOVED: The filter for 'strong' signals. We want the signal text for ANY winner.
    """
    all_rec_jsons = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=RECOMMENDATION_PREFIX)
    json_paths = [path for path in all_rec_jsons if path.endswith('.json')]
    
    if not json_paths:
        logging.warning("No recommendation JSON files found.")
        return pd.DataFrame()

    def read_json_blob(blob_name):
        try:
            content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
            return json.loads(content) if content else None
        except Exception as e:
            logging.error(f"Failed to read or parse {blob_name}: {e}")
            return None

    all_data = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        future_to_path = {executor.submit(read_json_blob, path): path for path in json_paths}
        for future in as_completed(future_to_path):
            data = future.result()
            if data:
                all_data.append(data)

    if not all_data:
        logging.warning("Could not parse any recommendation data.")
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    
    if 'outlook_signal' not in df.columns:
         return pd.DataFrame()

    # Sort by date and keep latest per ticker
    latest_df = df.sort_values('run_date', ascending=False).drop_duplicates('ticker')
    
    return latest_df[['ticker', 'outlook_signal', 'run_date']]

def _get_strong_options_setups() -> pd.DataFrame:
    """
    Queries the options signals table to find 'Strong' setups.
    Returns ONLY the #1 highest scoring contract per ticker.
    This includes Tier 1 (Fundamental) AND Tier 3 (ML Sniper) picks.
    """
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    
    query = f"""
        WITH RankedOptions AS (
            SELECT 
                ticker,
                contract_symbol,
                option_type,
                strike_price,
                expiration_date,
                setup_quality_signal,
                volatility_comparison_signal,
                summary,
                options_score,
                -- Rank contracts by score within each ticker
                ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY options_score DESC) as rn
            FROM `{SIGNALS_TABLE_ID}`
            WHERE (
                setup_quality_signal = 'Strong' -- Accepts Tier 1, 2, AND 3 (ML Sniper)
                OR (setup_quality_signal = 'Fair' AND summary LIKE '%ML SNIPER ALERT%') -- Accepts Tier 3 (ML Sniper) only
            )
              AND run_date = (SELECT MAX(run_date) FROM `{SIGNALS_TABLE_ID}`)
        )
        SELECT 
            ticker,
            contract_symbol,
            option_type,
            strike_price,
            expiration_date,
            setup_quality_signal,
            volatility_comparison_signal,
            summary,
            options_score
        FROM RankedOptions
        WHERE rn = 1
    """
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        logging.error(f"Failed to query strong options setups: {e}")
        return pd.DataFrame()

def _get_daily_predictions(tickers: list) -> pd.DataFrame:
    """
    Fetches the latest ML predictions (prob, prediction, contract_type).
    """
    if not tickers:
        return pd.DataFrame()

    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    
    query = f"""
        SELECT
            ticker,
            prob,
            prediction,
            CASE 
                WHEN UPPER(contract_type) IN ('LONG', 'CALL') THEN 'CALL'
                WHEN UPPER(contract_type) IN ('SHORT', 'PUT') THEN 'PUT'
                ELSE UPPER(contract_type)
            END AS contract_type
        FROM `{DAILY_PREDICTIONS_TABLE_ID}`
        WHERE ticker IN UNNEST(@tickers)
        QUALIFY ROW_NUMBER() OVER(PARTITION BY ticker, contract_type ORDER BY date DESC) = 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("tickers", "STRING", tickers),
        ]
    )
    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        return df
    except Exception as e:
        logging.error(f"Failed to query daily predictions: {e}")
        return pd.DataFrame()

def _get_asset_metadata_for_winners(tickers: list) -> pd.DataFrame:
    """
    Fetches all required asset metadata for the final list of winner tickers.
    """
    if not tickers:
        return pd.DataFrame()
        
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    query = f"""
        SELECT
            ticker,
            company_name,
            image_uri,
            price AS last_close,
            thirty_day_change_pct,
            industry,
            weighted_score
        FROM `{ASSET_METADATA_TABLE_ID}`
        WHERE ticker IN UNNEST(@tickers)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("tickers", "STRING", tickers),
        ]
    )
    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        return df
    except Exception as e:
        logging.error(f"Failed to query asset metadata: {e}")
        return pd.DataFrame()

def run_pipeline():
    """
    Orchestrates the creation of the 'winners_dashboard' table.
    Logic: If it is a 'Strong' option setup (verified by Analyzer), it goes on the board.
    We append Context (Recs) and Predictions (ML) if available.
    """
    logging.info("--- Starting Winners Dashboard Generation Pipeline (Relaxed) ---")

    # 1. Gather The Winners (Source of Truth is Options Analyzer)
    strong_options_df = _get_strong_options_setups()
    
    # Define final schema
    final_columns = [
        "image_uri", "company_name", "ticker", "outlook_signal",
        "last_close", "thirty_day_change_pct", "industry", "run_date", "weighted_score",
        # Contract Fields
        "contract_symbol", "option_type", "strike_price", "expiration_date",
        "setup_quality_signal", "volatility_comparison_signal", "summary", "options_score",
        # ML Prediction Fields
        "prob", "prediction"
    ]
    empty_df = pd.DataFrame(columns=final_columns)

    if strong_options_df.empty:
        logging.warning("No strong options signals found. Winners table will be cleared.")
        bq.load_df_to_bq(empty_df, OUTPUT_TABLE_ID, config.SOURCE_PROJECT_ID, write_disposition="WRITE_TRUNCATE")
        return

    # 2. Fetch Context: Recommendations (Outlook Signal)
    # We use ALL recommendations, so even "Neutral" outlooks are attached to Sniper trades
    recs_df = _get_all_stock_recommendations()

    # 3. Fetch Context: ML Predictions
    candidate_tickers = strong_options_df['ticker'].unique().tolist()
    predictions_df = _get_daily_predictions(candidate_tickers)

    # 4. Merge Data (Left Joins to preserve the Winners)
    
    # Merge Recommendations (Outlook)
    # We LEFT JOIN because if a Rec is missing, we still want the trade (it's likely a Sniper pick)
    winners_with_recs = pd.merge(strong_options_df, recs_df, on='ticker', how='left')

    # Merge Predictions (Probabilities)
    # We need to match Ticker AND Option Type to get the right probability
    if not predictions_df.empty:
        predictions_df['option_type'] = predictions_df['contract_type'].astype(str).str.lower()
        
        # LEFT JOIN: Keep the winner even if it's a Tier 1 trade with no ML prediction
        winners_complete = pd.merge(
            winners_with_recs,
            predictions_df[['ticker', 'option_type', 'prob', 'prediction']], 
            on=['ticker', 'option_type'], 
            how='left'
        )
    else:
        winners_complete = winners_with_recs
        winners_complete['prob'] = None
        winners_complete['prediction'] = None

    if winners_complete.empty:
        logging.warning("Merge resulted in empty dataset.")
        bq.load_df_to_bq(empty_df, OUTPUT_TABLE_ID, config.SOURCE_PROJECT_ID, write_disposition="WRITE_TRUNCATE")
        return

    # 5. Fetch Metadata
    final_tickers = winners_complete['ticker'].unique().tolist()
    asset_metadata_df = _get_asset_metadata_for_winners(final_tickers)

    if asset_metadata_df.empty:
        logging.error("Could not retrieve asset metadata. Clearing table.")
        bq.load_df_to_bq(empty_df, OUTPUT_TABLE_ID, config.SOURCE_PROJECT_ID, write_disposition="WRITE_TRUNCATE")
        return

    # 6. Final Assembly
    final_df = pd.merge(winners_complete, asset_metadata_df, on='ticker', how='left')
    
    # Fill missing cols
    for col in final_columns:
        if col not in final_df.columns:
            final_df[col] = None
            
    final_df = final_df[final_columns]

    logging.info(f"Generated {len(final_df)} winning rows. Loading to BigQuery...")
    bq.load_df_to_bq(final_df, OUTPUT_TABLE_ID, config.SOURCE_PROJECT_ID, write_disposition="WRITE_TRUNCATE")
    
    logging.info(f"--- Winners Dashboard Generation Pipeline Finished ---")