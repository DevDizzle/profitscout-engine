# serving/core/pipelines/performance_tracker_updater.py
import logging
from datetime import date
import pandas as pd
from google.cloud import bigquery
from .. import config
import numpy as np

# --- Configuration ---
# 1. The Signals table (Output of Analyzer) - Source of Truth for "Strong" status
SIGNALS_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_analysis_signals"
)
# 2. The Candidates table (Output of Selector) - Source of Pricing (Bid/Ask)
CANDIDATES_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_candidates"
)
# 3. The Winners table (Output of Dashboard) - Source of Ticker Validation
WINNERS_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.winners_dashboard"
)
# 4. The Options Chain (Live Data) - For current price updates
OPTIONS_CHAIN_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_chain"
)
# 5. The Performance Tracker (Destination)
TRACKER_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.performance_tracker"
)


def _get_new_signals_and_active_contracts(
    bq_client: bigquery.Client,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    1. Fetches "Strong" signals from the last 3 days.
       - Validates against winners_dashboard.
       - Joins with options_candidates to get the original Bid/Ask.
    2. Fetches ALL contracts currently marked as 'Active' for updates.
    """
    today_iso = date.today().isoformat()

    # Query 1: Fetch NEW signals.
    # We join SIGNALS (Validation) + CANDIDATES (Pricing) + WINNERS (Filter)
    new_signals_query = f"""
        SELECT
            s.contract_symbol,
            s.ticker,
            CAST(s.run_date AS DATE) as run_date,
            CAST(s.expiration_date AS DATE) as expiration_date,
            s.option_type,
            s.strike_price,
            s.stock_price_trend_signal,
            s.setup_quality_signal,
            
            -- Pull Pricing from the CANDIDATES table (snapshot at creation time)
            c.bid as signal_bid,
            c.ask as signal_ask,
            c.last_price as signal_last,
            
            w.company_name,
            w.industry,
            w.image_uri
        FROM `{SIGNALS_TABLE_ID}` s
        
        -- Filter: Must be on the Winners Dashboard
        JOIN `{WINNERS_TABLE_ID}` w ON s.ticker = w.ticker
        
        -- Pricing: Get original pricing from Candidates table
        -- We match on Symbol AND Date to ensure we get the price from that specific run
        JOIN `{CANDIDATES_TABLE_ID}` c 
          ON s.contract_symbol = c.contract_symbol 
          AND DATE(s.run_date) = DATE(c.selection_run_ts)
          
        -- Check if already tracked
        LEFT JOIN `{TRACKER_TABLE_ID}` t ON s.contract_symbol = t.contract_symbol
        
        WHERE CAST(s.run_date AS DATE) >= DATE_SUB(@today, INTERVAL 3 DAY)
          AND s.setup_quality_signal = 'Strong'
          AND (
            (s.stock_price_trend_signal LIKE '%Bullish%' AND s.option_type = 'call') OR
            (s.stock_price_trend_signal LIKE '%Bearish%' AND s.option_type = 'put')
          )
          AND (t.contract_symbol IS NULL OR t.status != 'Active')
    """

    active_contracts_query = f"""
        SELECT
            contract_symbol,
            run_date,
            expiration_date,
            initial_price,
            ticker,
            option_type,
            strike_price,
            stock_price_trend_signal,
            setup_quality_signal,
            company_name,
            industry,
            image_uri
        FROM `{TRACKER_TABLE_ID}`
        WHERE status = 'Active'
    """

    job_config_new = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("today", "DATE", today_iso)]
    )

    logging.info("Fetching new signals (Signal + Candidate Join)...")
    new_signals_df = bq_client.query(
        new_signals_query, job_config=job_config_new
    ).to_dataframe()
    logging.info(f"Found {len(new_signals_df)} new contracts to start tracking.")

    logging.info("Fetching active tracked contracts...")
    active_contracts_df = bq_client.query(active_contracts_query).to_dataframe()
    logging.info(f"Found {len(active_contracts_df)} active contracts.")

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
                 # PROTECT INITIAL_PRICE: Never overwrite it once set
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

    # 1. Fetch Latest Market Prices (for active updates)
    all_symbols = []
    if not new_signals_df.empty:
        all_symbols.extend(new_signals_df['contract_symbol'].tolist())
    if not active_contracts_df.empty:
        all_symbols.extend(active_contracts_df['contract_symbol'].tolist())
    
    current_prices_df = pd.DataFrame()
    if all_symbols:
        current_prices_df = _get_current_prices(bq_client, list(set(all_symbols)))

    # 2. Process NEW Signals (Lock in Initial Price from Candidate Data)
    processed_new = []
    if not new_signals_df.empty:
        # Calculate Initial Price from the CANDIDATE snapshot (Midpoint)
        new_signals_df['initial_price'] = (new_signals_df['signal_bid'] + new_signals_df['signal_ask']) / 2
        new_signals_df['initial_price'] = new_signals_df['initial_price'].fillna(new_signals_df['signal_last'])
        
        new_signals_df['status'] = "Active"
        new_signals_df['percent_gain'] = 0.0
        new_signals_df['last_updated'] = pd.Timestamp.utcnow()
        
        # Set Current Price (Use today's price if available, else initial)
        new_signals_df = pd.merge(new_signals_df, current_prices_df, on='contract_symbol', how='left')
        new_signals_df['current_price'] = new_signals_df['current_price'].fillna(new_signals_df['initial_price'])
        
        new_signals_df.dropna(subset=['initial_price'], inplace=True)
        processed_new.append(new_signals_df)

    # 3. Process ACTIVE Contracts (Update Price/Status)
    processed_active = []
    if not active_contracts_df.empty:
        active_contracts_df = pd.merge(active_contracts_df, current_prices_df, on='contract_symbol', how='left')
        
        # Update Gains
        mask = (active_contracts_df["initial_price"] > 0) & (active_contracts_df["current_price"].notna())
        active_contracts_df.loc[mask, "percent_gain"] = (
             (active_contracts_df.loc[mask, "current_price"] - active_contracts_df.loc[mask, "initial_price"]) /
              active_contracts_df.loc[mask, "initial_price"] * 100
        )
        
        # Check Expiration/Delisting
        active_contracts_df["expiration_date"] = pd.to_datetime(active_contracts_df["expiration_date"], errors='coerce').dt.date
        expired = (active_contracts_df["expiration_date"] < today)
        active_contracts_df.loc[expired, "status"] = "Expired"
        
        # Delisted if price is missing and not expired
        active_contracts_df.loc[active_contracts_df["current_price"].isna() & ~expired, "status"] = "Delisted"
        
        active_contracts_df['last_updated'] = pd.Timestamp.utcnow()
        processed_active.append(active_contracts_df)

    # 4. Upsert
    all_dfs = processed_new + processed_active
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df.drop_duplicates(subset=['contract_symbol'], keep='last', inplace=True)
        final_df['last_updated'] = pd.to_datetime(final_df['last_updated'], utc=True)
        _upsert_with_merge(bq_client, final_df)
    else:
        logging.info("No updates needed.")

    logging.info("--- Performance Tracker Pipeline Finished ---")
