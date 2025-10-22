# serving/core/pipelines/performance_tracker_updater.py
import logging
from datetime import date
import pandas as pd
from google.cloud import bigquery
from .. import config
import numpy as np # Import numpy for NaN handling

# --- Configuration ---
SIGNALS_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_analysis_signals"
)
OPTIONS_CHAIN_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_chain"
)
TRACKER_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.performance_tracker"
)
WINNERS_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.winners_dashboard"
)


def _get_new_signals_and_active_contracts(
    bq_client: bigquery.Client,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    1. Fetches today's new "Strong" signals if the underlying ticker was on the winners_dashboard today.
    2. Fetches ALL contracts currently marked as 'Active' in the tracker, regardless of today's winners list.
    """
    today_iso = date.today().isoformat()

    # Query 1: Fetch NEW signals for today's winners that aren't already tracked.
    # This query identifies contracts to START tracking today.
    new_signals_query = f"""
        SELECT
            s.contract_symbol,
            s.ticker,
            CAST(s.run_date AS DATE) as run_date, -- The date the signal was generated
            CAST(s.expiration_date AS DATE) as expiration_date,
            s.option_type,
            s.strike_price,
            s.stock_price_trend_signal,
            s.setup_quality_signal,
            w.company_name,
            w.industry,
            w.image_uri
        FROM `{SIGNALS_TABLE_ID}` s
        -- Ensure the signal's ticker and run_date match a winner entry for TODAY
        JOIN `{WINNERS_TABLE_ID}` w ON s.ticker = w.ticker AND CAST(s.run_date AS DATE) = CAST(w.run_date AS DATE)
        -- Ensure the contract isn't already in the tracker
        LEFT JOIN `{TRACKER_TABLE_ID}` t ON s.contract_symbol = t.contract_symbol
        WHERE CAST(s.run_date AS DATE) = @today -- Signal generated today
          AND s.setup_quality_signal = 'Strong'
          AND (
            (s.stock_price_trend_signal LIKE '%Bullish%' AND s.option_type = 'call') OR
            (s.stock_price_trend_signal LIKE '%Bearish%' AND s.option_type = 'put')
          )
          AND t.contract_symbol IS NULL -- Only add if it's not already tracked
    """

    # Query 2: Fetch ALL currently active contracts from the tracker to update their status.
    # We don't filter by winners_dashboard here, as we continue tracking once started.
    active_contracts_query = f"""
        SELECT
            contract_symbol,
            run_date,          -- Original run_date when it was added
            expiration_date,
            initial_price,
            -- Include necessary fields needed later for merging/inserting if they somehow weren't there
            ticker,
            option_type,
            strike_price,
            stock_price_trend_signal,
            setup_quality_signal,
            company_name,
            industry,
            image_uri
        FROM `{TRACKER_TABLE_ID}`
        WHERE status = 'Active' -- Only fetch contracts that are currently being tracked
    """

    job_config_new = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("today", "DATE", today_iso)]
    )

    logging.info("Fetching new signals for today's winners...")
    new_signals_df = bq_client.query(
        new_signals_query, job_config=job_config_new
    ).to_dataframe()
    logging.info(f"Found {len(new_signals_df)} new contracts to start tracking.")

    logging.info("Fetching all active tracked contracts for updates...")
    # No date parameter needed for active contracts query
    active_contracts_df = bq_client.query(active_contracts_query).to_dataframe()
    logging.info(f"Found {len(active_contracts_df)} active contracts to update.")

    return new_signals_df, active_contracts_df


def _get_current_prices(
    bq_client: bigquery.Client, contract_symbols: list[str]
) -> pd.DataFrame:
    """Fetches the latest mid-price for a list of contract symbols."""
    if not contract_symbols:
        return pd.DataFrame(columns=['contract_symbol', 'current_price']) # Return empty DF with expected columns

    # Get the most recent fetch_date available in the options chain table
    max_fetch_date_query = f"SELECT MAX(fetch_date) as max_date FROM `{OPTIONS_CHAIN_TABLE_ID}`"
    max_fetch_date_result = list(bq_client.query(max_fetch_date_query).result())
    if not max_fetch_date_result or max_fetch_date_result[0]['max_date'] is None:
        logging.warning("Could not determine the latest fetch_date from options_chain. Cannot fetch current prices.")
        return pd.DataFrame(columns=['contract_symbol', 'current_price'])

    latest_fetch_date = max_fetch_date_result[0]['max_date']
    logging.info(f"Fetching current prices from options_chain for fetch_date: {latest_fetch_date}")

    query = f"""
        SELECT
            contract_symbol,
            -- Use midpoint as current price, ensure bid/ask are positive
            SAFE_DIVIDE(bid + ask, 2) AS current_price
        FROM `{OPTIONS_CHAIN_TABLE_ID}`
        WHERE contract_symbol IN UNNEST(@contract_symbols)
          -- Get data ONLY from the absolute latest fetch available
          AND fetch_date = @latest_fetch_date
          AND bid > 0 AND ask > 0 -- Require valid bid/ask for midpoint
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("contract_symbols", "STRING", contract_symbols),
            bigquery.ScalarQueryParameter("latest_fetch_date", "DATE", latest_fetch_date)
        ]
    )
    return bq_client.query(query, job_config=job_config).to_dataframe()


def _upsert_with_merge(bq_client: bigquery.Client, df: pd.DataFrame):
    """Upserts the DataFrame into the performance tracker table using MERGE."""
    if df.empty:
        logging.info("No data to upsert. Skipping MERGE operation.")
        return

    # Ensure date columns are date objects for BQ compatibility
    for col in ["run_date", "expiration_date"]:
        if col in df.columns:
            # Handle potential NaT (Not a Time) values before conversion
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date


    temp_table_id = f"{TRACKER_TABLE_ID}_temp_staging_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    try:
        logging.info(f"Loading data to temporary table: {temp_table_id}")
        bq_client.load_table_from_dataframe(
            df, temp_table_id, job_config=job_config
        ).result()
        logging.info("Temporary table load complete.")
    except Exception as e:
        logging.error(f"Failed to load to temporary table {temp_table_id}: {e}", exc_info=True)
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        raise

    # Define columns for INSERT - ensure all columns from df are included
    # List expected columns explicitly to avoid errors if df has extra ones
    expected_cols_for_table = [
        'contract_symbol', 'ticker', 'run_date', 'expiration_date', 'option_type',
        'strike_price', 'stock_price_trend_signal', 'setup_quality_signal',
        'initial_price', 'current_price', 'percent_gain', 'status', 'last_updated',
        'company_name', 'industry', 'image_uri'
    ]
    all_columns = [f"`{col}`" for col in df.columns if col in expected_cols_for_table]

    insert_columns_str = ", ".join(all_columns)
    # Match source columns to the order of insert columns
    source_columns_str = ", ".join([f"S.`{col.strip('`')}`" for col in all_columns])


    # --- FIX 2: Use COALESCE to "freeze" old values ---
    # This modifies the UPDATE SET clause to only update price/gain
    # if the source (S) has a non-null value. Otherwise, it keeps
    # the existing target (T) value.
    merge_sql = f"""
    MERGE `{TRACKER_TABLE_ID}` T
    USING `{temp_table_id}` S ON T.contract_symbol = S.contract_symbol
    WHEN MATCHED THEN
        -- Only update fields relevant to ongoing tracking for existing active contracts
        UPDATE SET
            -- If S.current_price is NULL (delisted), keep T.current_price
            T.current_price = COALESCE(S.current_price, T.current_price),
            -- If S.percent_gain is NULL (delisted), keep T.percent_gain
            T.percent_gain = COALESCE(S.percent_gain, T.percent_gain),
            
            T.status = S.status, -- Always update status (e.g., Active -> Expired/Delisted)
            T.last_updated = CURRENT_TIMESTAMP() -- Use BQ function directly here for TIMESTAMP
            -- Do NOT update initial_price, run_date, ticker etc. for matched rows
    WHEN NOT MATCHED THEN
        -- Insert all columns for new contracts being added
        -- Ensure last_updated is handled correctly for INSERT
        INSERT ({insert_columns_str})
        VALUES ({source_columns_str}) -- Assumes last_updated column exists in S correctly typed,
                                        -- BQ load handles Pandas Timestamp with TZ to BQ TIMESTAMP
    """
    # --- END FIX 2 ---
    
    try:
        logging.info(f"Executing MERGE into {TRACKER_TABLE_ID}...")
        merge_job = bq_client.query(merge_sql)
        merge_job.result()
        logging.info(
            f"MERGE complete. {merge_job.num_dml_affected_rows} rows affected in {TRACKER_TABLE_ID}."
        )
    except Exception as e:
         logging.error(f"MERGE operation failed: {e}", exc_info=True)
         # Extract specific error message if available
         error_detail = getattr(e, 'message', str(e))
         logging.error(f"BigQuery error detail: {error_detail}")
         raise
    finally:
        logging.info(f"Deleting temporary table: {temp_table_id}")
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        logging.info("Temporary table deleted.")


def run_pipeline():
    """Main function to run the performance tracker update pipeline."""
    logging.info("--- Starting Performance Tracker Update Pipeline ---")
    bq_client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    today = date.today()

    try:
        new_signals_df, active_contracts_df = _get_new_signals_and_active_contracts(bq_client)

        # Combine symbols from both new and active contracts for price fetching
        all_symbols_to_fetch = []
        if not new_signals_df.empty:
            all_symbols_to_fetch.extend(new_signals_df['contract_symbol'].tolist())
        if not active_contracts_df.empty:
            all_symbols_to_fetch.extend(active_contracts_df['contract_symbol'].tolist())

        # Remove duplicates
        unique_symbols_to_fetch = list(set(all_symbols_to_fetch))

        # Fetch current prices for all relevant contracts in one go
        current_prices_df = pd.DataFrame() # Initialize as empty
        if unique_symbols_to_fetch:
              logging.info(f"Fetching current prices for {len(unique_symbols_to_fetch)} unique contracts...")
              current_prices_df = _get_current_prices(bq_client, unique_symbols_to_fetch)
              logging.info(f"Fetched {len(current_prices_df)} current prices.")
        else:
            logging.info("No new or active contracts identified. Skipping price fetch.")


        # --- Process NEW signals to be added ---
        processed_new_signals_list = []
        if not new_signals_df.empty:
            # Merge fetched prices to get the initial_price
            new_signals_df = pd.merge(
                new_signals_df,
                current_prices_df[['contract_symbol', 'current_price']], # Select only needed columns
                on="contract_symbol",
                how="left" # Keep all new signals, price might be NaN if not found
            )
            new_signals_df.rename(columns={"current_price": "initial_price"}, inplace=True)
            new_signals_df["current_price"] = new_signals_df["initial_price"] # Initially same
            new_signals_df["status"] = "Active"
            new_signals_df["percent_gain"] = 0.0
            # FIX: Use timezone-aware UTC timestamp from Pandas
            new_signals_df["last_updated"] = pd.Timestamp.utcnow()

            # Filter out rows where initial price couldn't be fetched - cannot track without it
            initial_count = len(new_signals_df)
            new_signals_df.dropna(subset=['initial_price'], inplace=True)
            dropped_count = initial_count - len(new_signals_df)
            if dropped_count > 0:
                logging.warning(f"Dropped {dropped_count} new signals due to missing initial price in options_chain.")

            processed_new_signals_list.append(new_signals_df)


        # --- Process ACTIVE contracts to be updated ---
        processed_active_contracts_list = []
        if not active_contracts_df.empty:
            initial_active_count = len(active_contracts_df)
            # Merge current prices fetched earlier
            active_contracts_df = pd.merge(
                active_contracts_df,
                current_prices_df[['contract_symbol', 'current_price']], # Select only needed cols
                on="contract_symbol",
                how="left" # Keep all active contracts, price might be NaN if not found today
            )

            # Calculate percent gain - handle NaN/None initial_price or zero initial_price
            # Calculate only if current_price is also available
            mask = (active_contracts_df["initial_price"].notna()) & \
                   (active_contracts_df["initial_price"] != 0) & \
                   (active_contracts_df["current_price"].notna())

            active_contracts_df["percent_gain"] = np.nan # Default to NaN
            active_contracts_df.loc[mask, "percent_gain"] = (
                 (active_contracts_df.loc[mask, "current_price"] - active_contracts_df.loc[mask, "initial_price"]) /
                  active_contracts_df.loc[mask, "initial_price"] * 100
            )

            active_contracts_df["status"] = "Active" # Assume active initially

            # Ensure expiration_date is a date object for comparison
            active_contracts_df["expiration_date"] = pd.to_datetime(
                active_contracts_df["expiration_date"], errors='coerce'
            ).dt.date

            # Mark as Expired if expiration date is past (and not null)
            expired_mask = (active_contracts_df["expiration_date"].notna()) & (active_contracts_df["expiration_date"] < today)
            active_contracts_df.loc[expired_mask, "status"] = "Expired"

            # Mark as Delisted if current_price is NaN after the merge (wasn't found in latest options_chain)
            # AND it's not already marked as Expired
            delisted_mask = active_contracts_df["current_price"].isna() & (~expired_mask)
            active_contracts_df.loc[delisted_mask, "status"] = "Delisted"

            # FIX: Use timezone-aware UTC timestamp from Pandas
            active_contracts_df["last_updated"] = pd.Timestamp.utcnow()

            # Keep only the rows that need updating (status changed OR still Active/Delisted/Expired)
            # We essentially keep everything processed here to pass to MERGE
            processed_active_contracts_list.append(active_contracts_df)


        # --- Combine and Upsert ---
        all_processed_dfs = processed_new_signals_list + processed_active_contracts_list

        if not all_processed_dfs:
            logging.info("No contracts to add or update in the performance tracker.")
            final_df = pd.DataFrame() # Ensure final_df exists but is empty
        else:
            # Concatenate all processed dataframes
            final_df = pd.concat(all_processed_dfs, ignore_index=True, sort=False)

            # --- FIX 1: Deduplicate to prevent MERGE error ---
            # This ensures only one source row per contract_symbol.
            # We keep 'last' because active contracts are concatenated last,
            # prioritizing their updates over any new_signal overlap.
            if not final_df.empty:
                initial_row_count = len(final_df)
                final_df.drop_duplicates(subset=['contract_symbol'], keep='last', inplace=True)
                dropped_row_count = initial_row_count - len(final_df)
                if dropped_row_count > 0:
                    logging.warning(f"Dropped {dropped_row_count} duplicate contract_symbol rows from final_df before upsert.")
            # --- END FIX 1 ---

            # Ensure essential columns exist, fill potentially missing metadata for active rows if needed (though unlikely)
            required_cols = [
                'contract_symbol', 'ticker', 'run_date', 'expiration_date', 'option_type',
                'strike_price', 'stock_price_trend_signal', 'setup_quality_signal',
                'initial_price', 'current_price', 'percent_gain', 'status', 'last_updated',
                'company_name', 'industry', 'image_uri'
            ]
            for col in required_cols:
                if col not in final_df.columns:
                    final_df[col] = None # Add missing columns with None/NaN

            # Reorder columns for consistency before loading to BQ
            final_df = final_df[required_cols]


        if final_df.empty:
            logging.info("No contracts processed for upsert.")
        else:
            logging.info(
                f"Preparing to upsert {len(final_df)} records (new & updated) into the performance tracker."
            )
            # Explicitly cast last_updated to datetime64[ns, UTC] just before loading BQ temp table
            final_df['last_updated'] = pd.to_datetime(final_df['last_updated'], utc=True)

            _upsert_with_merge(bq_client, final_df)

        logging.info("--- Performance Tracker Update Pipeline Finished ---")

    except Exception as e:
        logging.critical(f"Pipeline failed with error: {e}", exc_info=True)
        # Consider raising the exception if pipeline failure should halt workflow
        # raise e