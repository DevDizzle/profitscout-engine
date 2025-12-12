# serving/core/pipelines/performance_tracker_updater.py
import logging
from datetime import date
import pandas as pd
from google.cloud import bigquery
from .. import config
import numpy as np

# --- Configuration ---
# 1. The Winners table (Output of Dashboard) - SOURCE OF TRUTH for Selection
WINNERS_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.winners_dashboard"
)
# 2. The Candidates table (Output of Selector) - Source of Pricing (Bid/Ask)
CANDIDATES_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_candidates"
)
# 3. The Options Chain (Live Data) - For active monitoring/current price updates
OPTIONS_CHAIN_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_chain"
)
# 4. The Performance Tracker (Destination)
TRACKER_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.performance_tracker"
)


def _get_new_signals_and_active_contracts(
    bq_client: bigquery.Client,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    1. Fetches NEW contracts strictly from the Winners Dashboard.
    2. Fetches ALL UNEXPIRED contracts from the tracker (Active OR Delisted)
       to allow for price recovery/updates.
    """
    today_iso = date.today().isoformat()

    # Query 1: Fetch NEW signals directly from Winners Dashboard.
    new_signals_query = f"""
        SELECT
            w.contract_symbol,
            w.ticker,
            CAST(w.run_date AS DATE) as run_date,
            CAST(w.expiration_date AS DATE) as expiration_date,
            w.option_type,
            w.strike_price,
            w.outlook_signal as stock_price_trend_signal,
            w.setup_quality_signal,
            w.company_name,
            w.industry,
            w.image_uri,
            
            -- Pull Pricing from the CANDIDATES table (snapshot at creation time)
            c.bid as signal_bid,
            c.ask as signal_ask,
            c.last_price as signal_last
            
        FROM `{WINNERS_TABLE_ID}` w
        
        JOIN `{CANDIDATES_TABLE_ID}` c 
          ON w.contract_symbol = c.contract_symbol 
          AND CAST(w.run_date AS DATE) = DATE(c.selection_run_ts)
          
        LEFT JOIN `{TRACKER_TABLE_ID}` t ON w.contract_symbol = t.contract_symbol
        WHERE t.contract_symbol IS NULL
          AND CAST(w.expiration_date AS DATE) >= CURRENT_DATE()
        
        QUALIFY ROW_NUMBER() OVER(PARTITION BY w.contract_symbol ORDER BY c.selection_run_ts DESC) = 1
    """

    # Query 2: Fetch ALL Unexpired Contracts (Active OR Delisted)
    # This allows us to keep checking "Delisted" contracts in case data returns.
    active_contracts_query = f"""
        SELECT
            contract_symbol,
            run_date,
            expiration_date,
            initial_price,
            current_price, 
            percent_gain, 
            ticker,
            option_type,
            strike_price,
            stock_price_trend_signal,
            setup_quality_signal,
            company_name,
            industry,
            image_uri,
            status
        FROM `{TRACKER_TABLE_ID}`
        WHERE CAST(expiration_date AS DATE) >= CURRENT_DATE()
          AND status IN ('Active', 'Delisted')
    """

    logging.info(f"Fetching new signals from Winners Dashboard ({WINNERS_TABLE_ID})...")
    new_signals_df = bq_client.query(new_signals_query).to_dataframe()
    logging.info(f"Found {len(new_signals_df)} new contracts to start tracking.")

    logging.info("Fetching ongoing contracts (Active + Delisted) for updates...")
    active_contracts_df = bq_client.query(active_contracts_query).to_dataframe()
    logging.info(f"Found {len(active_contracts_df)} ongoing contracts to check.")

    return new_signals_df, active_contracts_df


def _get_current_prices(
    bq_client: bigquery.Client, contract_symbols: list[str]
) -> pd.DataFrame:
    """Fetches the latest mid-price from the Options Chain (Live/Morning Data)."""
    if not contract_symbols:
        return pd.DataFrame(columns=['contract_symbol', 'current_price'])

    max_fetch_date_query = f"SELECT MAX(fetch_date) as max_date FROM `{OPTIONS_CHAIN_TABLE_ID}`"
    max_fetch_date_result = list(bq_client.query(max_fetch_date_query).result())
    if not max_fetch_date_result or max_fetch_date_result[0]['max_date'] is None:
        return pd.DataFrame(columns=['contract_symbol', 'current_price'])

    latest_fetch_date = max_fetch_date_result[0]['max_date']
    
    query = f"""
        SELECT
            contract_symbol,
            SAFE_DIVIDE(bid + ask, 2) AS current_price
        FROM `{OPTIONS_CHAIN_TABLE_ID}`
        WHERE contract_symbol IN UNNEST(@contract_symbols)
          AND fetch_date = @latest_fetch_date
          AND bid > 0 AND ask > 0
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
        return

    for col in ["run_date", "expiration_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    temp_table_id = f"{TRACKER_TABLE_ID}_temp_staging_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    
    try:
        bq_client.load_table_from_dataframe(df, temp_table_id, job_config=job_config).result()
    except Exception as e:
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        raise

    expected_cols = [
        'contract_symbol', 'ticker', 'run_date', 'expiration_date', 'option_type',
        'strike_price', 'stock_price_trend_signal', 'setup_quality_signal',
        'initial_price', 'current_price', 'percent_gain', 'status', 'last_updated',
        'company_name', 'industry', 'image_uri'
    ]
    
    all_columns = [f"`{col}`" for col in df.columns if col in expected_cols]
    insert_cols = ", ".join(all_columns)
    source_cols = ", ".join([f"S.`{col.strip('`')}`" for col in all_columns])

    update_parts = []
    for col in df.columns:
        if col in expected_cols and col != 'contract_symbol':
            if col == 'last_updated':
                 update_parts.append(f"T.`{col}` = CURRENT_TIMESTAMP()")
            elif col in ['ticker', 'expiration_date', 'option_type', 'strike_price', 'initial_price']:
                 update_parts.append(f"T.`{col}` = COALESCE(T.`{col}`, S.`{col}`)")
            else:
                 update_parts.append(f"T.`{col}` = S.`{col}`")

    merge_sql = f"""
    MERGE `{TRACKER_TABLE_ID}` T
    USING `{temp_table_id}` S ON T.contract_symbol = S.contract_symbol
    WHEN MATCHED THEN UPDATE SET {", ".join(update_parts)}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({source_cols})
    """

    try:
        bq_client.query(merge_sql).result()
    finally:
        bq_client.delete_table(temp_table_id, not_found_ok=True)


def run_pipeline():
    logging.info("--- Starting Performance Tracker Update Pipeline ---")
    bq_client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    today = date.today()

    new_signals_df, active_contracts_df = _get_new_signals_and_active_contracts(bq_client)

    # --- SPLIT LOGIC: Separate ongoing from expired ---
    
    if not active_contracts_df.empty:
        active_contracts_df["expiration_date"] = pd.to_datetime(active_contracts_df["expiration_date"]).dt.date
    
    active_ongoing_df = pd.DataFrame()
    active_expired_df = pd.DataFrame() # Expired, need to freeze

    if not active_contracts_df.empty:
        # Check against today. If Exp Date >= Today, it's still alive.
        active_ongoing_df = active_contracts_df[active_contracts_df["expiration_date"] >= today].copy()
        # If Exp Date < Today, it has expired.
        active_expired_df = active_contracts_df[active_contracts_df["expiration_date"] < today].copy()

    logging.info(f"Ongoing Contracts to Update: {len(active_ongoing_df)}")
    logging.info(f"Expired Contracts to Freeze: {len(active_expired_df)}")

    # 1. Fetch Latest Market Prices (ONLY for new + ongoing)
    symbols_to_fetch = []
    if not new_signals_df.empty:
        symbols_to_fetch.extend(new_signals_df['contract_symbol'].tolist())
    if not active_ongoing_df.empty:
        symbols_to_fetch.extend(active_ongoing_df['contract_symbol'].tolist())
    
    current_prices_df = pd.DataFrame()
    if symbols_to_fetch:
        current_prices_df = _get_current_prices(bq_client, list(set(symbols_to_fetch)))

    # 2. Process NEW Signals
    processed_new = []
    if not new_signals_df.empty:
        # Calculate Initial Price
        new_signals_df['initial_price'] = (new_signals_df['signal_bid'] + new_signals_df['signal_ask']) / 2
        new_signals_df['initial_price'] = new_signals_df['initial_price'].fillna(new_signals_df['signal_last'])
        
        new_signals_df['status'] = "Active"
        new_signals_df['percent_gain'] = 0.0
        new_signals_df['last_updated'] = pd.Timestamp.utcnow()
        
        # Set Current Price (Use today's price if available, else fallback to initial)
        new_signals_df = pd.merge(new_signals_df, current_prices_df, on='contract_symbol', how='left')
        
        # NOTE: For new signals, if we have NO price data at all, we drop them. 
        # We don't want to track something that effectively doesn't exist yet.
        before_drop = len(new_signals_df)
        new_signals_df.dropna(subset=['initial_price'], inplace=True)
        
        # If current_price is missing for a NEW signal, use initial_price to avoid NaN
        new_signals_df['current_price'] = new_signals_df['current_price'].fillna(new_signals_df['initial_price'])

        if not new_signals_df.empty:
            processed_new.append(new_signals_df)

    # 3. Process ONGOING Contracts (Active OR Delisted)
    processed_ongoing = []
    if not active_ongoing_df.empty:
        # Keep 'old_price' to fill gaps
        active_ongoing_df.rename(columns={'current_price': 'old_price'}, inplace=True)

        # Merge with new prices
        active_ongoing_df = pd.merge(active_ongoing_df, current_prices_df, on='contract_symbol', how='left')
        
        # Coerce numeric
        for col in ['initial_price', 'current_price', 'old_price']:
             active_ongoing_df[col] = pd.to_numeric(active_ongoing_df[col], errors='coerce')

        # --- RECOVERY LOGIC ---
        # 1. If we HAVE a new price -> Status becomes 'Active' (Revival!)
        # 2. If we DO NOT have a new price -> Status becomes 'Delisted' (or stays Delisted)
        
        has_new_price = active_ongoing_df['current_price'].notna()
        active_ongoing_df.loc[has_new_price, 'status'] = 'Active'
        active_ongoing_df.loc[~has_new_price, 'status'] = 'Delisted'
        
        # Fill missing current_price with old_price
        active_ongoing_df['current_price'] = active_ongoing_df['current_price'].fillna(active_ongoing_df['old_price'])
        
        # Final fallback: if still NaN (no old price either), use initial_price or 0.0
        # This fixes the DB nulls while allowing the contract to persist.
        active_ongoing_df['current_price'] = active_ongoing_df['current_price'].fillna(active_ongoing_df['initial_price']).fillna(0.0)

        # Recalculate Gains
        mask = (active_ongoing_df["initial_price"] > 0)
        active_ongoing_df.loc[mask, "percent_gain"] = (
             (active_ongoing_df.loc[mask, "current_price"] - active_ongoing_df.loc[mask, "initial_price"]) /
              active_ongoing_df.loc[mask, "initial_price"] * 100
        ).round(2)
        
        if 'old_price' in active_ongoing_df.columns:
            active_ongoing_df.drop(columns=['old_price'], inplace=True)
        
        active_ongoing_df['last_updated'] = pd.Timestamp.utcnow()
        processed_ongoing.append(active_ongoing_df)

    # 4. Process EXPIRED Contracts (The Freeze)
    processed_expired = []
    if not active_expired_df.empty:
        active_expired_df["status"] = "Expired"
        
        # Fix NaNs for expired contracts too, just in case
        active_expired_df['current_price'] = pd.to_numeric(active_expired_df['current_price'], errors='coerce').fillna(0.0)
        active_expired_df['percent_gain'] = pd.to_numeric(active_expired_df['percent_gain'], errors='coerce').fillna(-100.0)

        active_expired_df["last_updated"] = pd.Timestamp.utcnow()
        processed_expired.append(active_expired_df)

    # 5. Upsert All
    all_dfs = processed_new + processed_ongoing + processed_expired
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df.drop_duplicates(subset=['contract_symbol'], keep='last', inplace=True)
        final_df['last_updated'] = pd.to_datetime(final_df['last_updated'], utc=True)
        _upsert_with_merge(bq_client, final_df)
    else:
        logging.info("No updates needed.")

    logging.info("--- Performance Tracker Pipeline Finished ---")