# serving/core/pipelines/winners_dashboard_generator.py
import logging
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from .. import config, gcs, bq # Ensure bq is imported if you use bq.load_df_to_bq

# --- Configuration ---
RECOMMENDATION_PREFIX = "recommendations/"
SIGNALS_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_analysis_signals"
)
# --- ADDED: Options Candidates Table ID ---
CANDIDATES_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_candidates"
)
ASSET_METADATA_TABLE_ID = (
    f"{config.DESTINATION_PROJECT_ID}.{config.BIGQUERY_DATASET}.asset_metadata"
)
OUTPUT_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.winners_dashboard"
)

# --- Main Logic ---


def _get_strong_stock_recommendations() -> pd.DataFrame:
    """
    Fetches latest strong stock recommendations (one per ticker).
    """
    all_rec_jsons = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=RECOMMENDATION_PREFIX)
    json_paths = [path for path in all_rec_jsons if path.endswith(".json")]

    if not json_paths:
        logging.warning("No recommendation JSON files found.")
        return pd.DataFrame()

    def read_json_blob(blob_name):
        try:
            content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
            data = json.loads(content) if content else None
            # Ensure essential fields are present
            if data and 'ticker' in data and 'outlook_signal' in data and 'run_date' in data:
                 # Only keep needed fields to avoid column conflicts later
                return {'ticker': data['ticker'], 'outlook_signal': data['outlook_signal'], 'rec_run_date': data['run_date']}
            return None
        except Exception as e:
            logging.error(f"Failed to read or parse {blob_name}: {e}")
            return None

    all_data = []
    # Reduced workers as GCS reads are usually fast
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_path = {
            executor.submit(read_json_blob, path): path for path in json_paths
        }
        for future in as_completed(future_to_path):
            data = future.result()
            if data:
                all_data.append(data)

    if not all_data:
        logging.warning("Could not parse any valid recommendation data.")
        return pd.DataFrame()

    df = pd.DataFrame(all_data)

    strong_signals = [
        "Strongly Bullish",
        "Moderately Bullish",
        "Strongly Bearish",
        "Moderately Bearish",
    ]
    filtered_df = df[df["outlook_signal"].isin(strong_signals)].copy() # Use .copy() to avoid SettingWithCopyWarning

    # Ensure rec_run_date is datetime for sorting
    filtered_df['rec_run_date'] = pd.to_datetime(filtered_df['rec_run_date'], errors='coerce')
    filtered_df.dropna(subset=['rec_run_date'], inplace=True)


    latest_df = filtered_df.sort_values("rec_run_date", ascending=False).drop_duplicates(
        "ticker", keep='first' # Keep the absolute latest if multiple exist
    )

    # We only need the ticker and the specific signal information from this step
    return latest_df[["ticker", "outlook_signal", "rec_run_date"]]


def _get_strong_options_signals_details() -> pd.DataFrame:
    """
    Queries the options signals table, joins with candidates to get options_score,
    and filters for 'Strong' setups matching directional criteria from the most recent run.
    """
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    # --- MODIFIED QUERY ---
    # Joins signals with the latest candidates run to fetch options_score
    query = f"""
        WITH LatestCandidates AS (
            -- Select candidates only from the very latest run
            SELECT *
            FROM `{CANDIDATES_TABLE_ID}`
            WHERE selection_run_ts = (SELECT MAX(selection_run_ts) FROM `{CANDIDATES_TABLE_ID}`)
        )
        SELECT
            s.ticker,
            s.contract_symbol,
            s.option_type,
            s.strike_price,
            CAST(s.run_date AS DATE) AS run_date,
            CAST(s.expiration_date AS DATE) AS expiration_date,
            s.stock_price_trend_signal,
            s.setup_quality_signal,
            c.options_score -- Fetched from candidates table
        FROM `{SIGNALS_TABLE_ID}` s
        INNER JOIN LatestCandidates c ON s.contract_symbol = c.contract_symbol -- Join based on contract
        WHERE s.setup_quality_signal = 'Strong'
          AND s.run_date = (SELECT MAX(run_date) FROM `{SIGNALS_TABLE_ID}`) -- Ensure signal is also from latest run
          AND ( -- Directional Alignment Check
                (s.stock_price_trend_signal LIKE '%Bullish%' AND s.option_type = 'call') OR
                (s.stock_price_trend_signal LIKE '%Bearish%' AND s.option_type = 'put')
              )
    """
    try:
        df = client.query(query).to_dataframe()
        logging.info(f"Fetched {len(df)} 'Strong' aligned option signals with scores from BQ.")
        # Handle potential missing scores if join fails for some reason
        if 'options_score' not in df.columns:
            df['options_score'] = pd.NA
        else:
            df['options_score'] = pd.to_numeric(df['options_score'], errors='coerce')

        return df
    except Exception as e:
        logging.error(f"Failed to query strong options signals details with scores: {e}", exc_info=True)
        return pd.DataFrame()


def _get_asset_metadata_for_winners(tickers: list) -> pd.DataFrame:
    """
    Fetches required asset metadata for the final list of winner tickers.
    """
    if not tickers:
        return pd.DataFrame()

    client = bigquery.Client(project=config.DESTINATION_PROJECT_ID) # Use DESTINATION project for asset_metadata
    # Select metadata fields needed for the dashboard
    query = f"""
        SELECT
            ticker,
            company_name,
            image_uri,
            price AS last_close,
            thirty_day_change_pct,
            industry,
            sector,
            weighted_score -- Stock weighted score
        FROM `{ASSET_METADATA_TABLE_ID}`
        WHERE ticker IN UNNEST(@tickers)
        -- Assuming asset_metadata is updated daily and doesn't need a date filter
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("tickers", "STRING", tickers),
        ]
    )
    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        logging.info(f"Fetched asset metadata for {len(df)} tickers.")
        return df
    except Exception as e:
        logging.error(f"Failed to query asset metadata: {e}", exc_info=True)
        return pd.DataFrame()


def run_pipeline():
    """
    Orchestrates the creation of the 'winners_dashboard' table by joining
    strong stock recommendations with *all* strong options setups (including options_score)
    for those tickers. The output table now has one row per contract_symbol.
    """
    logging.info("--- Starting Winners Dashboard Generation Pipeline (Contract Level with Options Score) ---")

    strong_recs_df = _get_strong_stock_recommendations()
    strong_options_df = _get_strong_options_signals_details()

    # --- MODIFIED: Added 'options_score' to the final columns ---
    final_columns = [
        "contract_symbol",
        "ticker",
        "company_name",
        "image_uri",
        "outlook_signal",
        "option_type",
        "strike_price",
        "expiration_date",
        "options_score", # Added options score for ranking
        "last_close",
        "thirty_day_change_pct",
        "industry",
        "sector",
        "run_date",
        "weighted_score", # Stock score
    ]
    empty_df = pd.DataFrame(columns=final_columns)

    if strong_recs_df.empty or strong_options_df.empty:
        logging.warning(
            "No strong stock recommendations or no strong options signals found. Winners table will be cleared."
        )
        bq.load_df_to_bq(
            empty_df,
            OUTPUT_TABLE_ID,
            config.SOURCE_PROJECT_ID,
            write_disposition="WRITE_TRUNCATE",
        )
        logging.info(
            "--- Winners Dashboard Generation Pipeline Finished (No Winners) ---"
        )
        return

    # Step 1: Merge strong options signals (now including options_score)
    # with strong stock recommendations on 'ticker'.
    winners_contracts_df = pd.merge(
        strong_options_df,
        strong_recs_df[['ticker', 'outlook_signal']],
        on="ticker",
        how="inner"
    )

    if winners_contracts_df.empty:
        logging.warning("No tickers matched between strong stocks and strong options. Final table will be empty.")
        bq.load_df_to_bq(
            empty_df,
            OUTPUT_TABLE_ID,
            config.SOURCE_PROJECT_ID,
            write_disposition="WRITE_TRUNCATE",
        )
        logging.info(
            "--- Winners Dashboard Generation Pipeline Finished (No Matches) ---"
        )
        return

    # Step 2: Get asset metadata for the unique tickers involved.
    winner_tickers = winners_contracts_df["ticker"].unique().tolist()
    asset_metadata_df = _get_asset_metadata_for_winners(winner_tickers)

    if asset_metadata_df.empty:
        logging.error(
            "Could not retrieve asset metadata for the winning tickers. Clearing table."
        )
        bq.load_df_to_bq(
            empty_df,
            OUTPUT_TABLE_ID,
            config.SOURCE_PROJECT_ID,
            write_disposition="WRITE_TRUNCATE",
        )
        return

    # Step 3: Merge the contract-level data with the asset metadata on 'ticker'.
    # The options_score from winners_contracts_df will be carried over.
    final_df = pd.merge(
        winners_contracts_df,
        asset_metadata_df,
        on="ticker",
        how="left"
    )

    # Ensure all required columns exist
    for col in final_columns:
        if col not in final_df.columns:
            final_df[col] = pd.NA

    # Select and reorder columns
    final_df = final_df[final_columns].copy()

    # Data Cleaning
    final_df.dropna(subset=['contract_symbol'], inplace=True)
    final_df.drop_duplicates(subset=['contract_symbol'], keep='first', inplace=True)

    # Optional: Sort by ticker then by options_score descending within each ticker group
    final_df.sort_values(by=['ticker', 'options_score'], ascending=[True, False], inplace=True, na_position='last')

    logging.info(f"Generated {len(final_df)} winning contracts with scores. Loading to BigQuery table {OUTPUT_TABLE_ID}...")
    bq.load_df_to_bq(
        final_df,
        OUTPUT_TABLE_ID,
        config.SOURCE_PROJECT_ID,
        write_disposition="WRITE_TRUNCATE",
    )

    logging.info(f"--- Winners Dashboard Generation Pipeline Finished ---")