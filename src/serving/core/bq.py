# serving/core/bq.py
import logging
import pandas as pd
from google.cloud import bigquery
import time

def load_df_to_bq(df: pd.DataFrame, table_id: str, project_id: str, write_disposition: str = "WRITE_TRUNCATE"):
    """
    Loads a pandas DataFrame into a BigQuery table.
    If the DataFrame is empty and the write disposition is TRUNCATE, it will wipe the table.
    """
    client = bigquery.Client(project=project_id)
    
    # --- THIS IS THE FIX ---
    # If the dataframe is empty but the goal is to truncate,
    # execute a direct TRUNCATE statement and exit.
    if df.empty and write_disposition == "WRITE_TRUNCATE":
        logging.info(f"DataFrame is empty. Truncating BigQuery table: {table_id}")
        try:
            client.query(f"TRUNCATE TABLE `{table_id}`").result()
            logging.info(f"Successfully truncated {table_id}.")
        except Exception as e:
            logging.error(f"Failed to truncate {table_id}: {e}", exc_info=True)
            raise
        return

    if df.empty:
        logging.warning("DataFrame is empty and write disposition is not TRUNCATE. Skipping BigQuery load.")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
    )
    if write_disposition == "WRITE_APPEND":
        job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
    
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logging.info(f"Loaded {job.output_rows} rows into BigQuery table: {table_id} using {write_disposition}")
    except Exception as e:
        logging.error(f"Failed to load DataFrame to {table_id}: {e}", exc_info=True)
        raise

def upsert_df_to_bq(df: pd.DataFrame, table_id: str, project_id: str):
    """
    Upserts a DataFrame into a BigQuery table using a MERGE statement.
    """
    if df.empty:
        logging.warning("DataFrame is empty. Skipping BigQuery MERGE operation.")
        return

    client = bigquery.Client(project=project_id)
    
    dataset_id = table_id.split('.')[-2]
    final_table_name = table_id.split('.')[-1]
    
    temp_table_name = f"{final_table_name}_temp_{int(time.time())}"
    temp_table_id = f"{project_id}.{dataset_id}.{temp_table_name}"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    try:
        load_job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
        load_job.result()
    except Exception as e:
        logging.error(f"Failed to load DataFrame to temp table {temp_table_id}: {e}", exc_info=True)
        raise

    cols_to_insert = ", ".join([f"`{col}`" for col in df.columns])
    cols_to_update = ", ".join([f"T.`{col}` = S.`{col}`" for col in df.columns if col != 'ticker'])
    
    merge_sql = f"""
    MERGE `{table_id}` T
    USING `{temp_table_id}` S ON T.ticker = S.ticker
    WHEN MATCHED THEN
      UPDATE SET {cols_to_update}
    WHEN NOT MATCHED THEN
      INSERT ({cols_to_insert}) VALUES ({cols_to_insert})
    """

    try:
        logging.info(f"Executing MERGE to upsert data into {table_id}...")
        merge_job = client.query(merge_sql)
        merge_job.result()
        logging.info(f"MERGE complete. {merge_job.num_dml_affected_rows} rows affected in {table_id}.")
    except Exception as e:
        logging.error(f"Failed to execute MERGE on {table_id}: {e}", exc_info=True)
        raise
    finally:
        client.delete_table(temp_table_id, not_found_ok=True)

def fetch_options_market_structure(ticker: str) -> dict:
    """
    Aggregates the raw options chain to identify structural walls and flow sentiment.
    Returns a dictionary suitable for LLM context or Dashboard widgets.
    """
    from . import config  # lazy import to avoid circular dependency
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    
    query = f"""
    WITH LatestChain AS (
        SELECT 
            *,
            DATE_DIFF(expiration_date, fetch_date, DAY) as dte
        FROM `{config.SOURCE_OPTIONS_CHAIN_TABLE_ID}`
        WHERE ticker = @ticker
          AND fetch_date = (
              SELECT MAX(fetch_date) 
              FROM `{config.SOURCE_OPTIONS_CHAIN_TABLE_ID}` 
              WHERE ticker = @ticker
          )
    ),
    SentimentStats AS (
        SELECT
            SUM(CASE WHEN LOWER(option_type) = 'call' THEN volume ELSE 0 END) as total_call_vol,
            SUM(CASE WHEN LOWER(option_type) = 'put' THEN volume ELSE 0 END) as total_put_vol,
            SUM(CASE WHEN LOWER(option_type) = 'call' THEN open_interest ELSE 0 END) as total_call_oi,
            SUM(CASE WHEN LOWER(option_type) = 'put' THEN open_interest ELSE 0 END) as total_put_oi,
            -- Rough GEX Proxy: Gamma * OI
            SUM(CASE WHEN LOWER(option_type) = 'call' THEN IFNULL(gamma,0) * open_interest ELSE 0 END) as net_call_gamma,
            SUM(CASE WHEN LOWER(option_type) = 'put' THEN IFNULL(gamma,0) * open_interest ELSE 0 END) as net_put_gamma
        FROM LatestChain
    ),
    Walls AS (
        SELECT
            -- Call Wall: Strike with highest Call Net Gamma (Resistance) - updated to avoid deep ITM strikes
            (SELECT strike FROM LatestChain WHERE LOWER(option_type) = 'call' ORDER BY (IFNULL(gamma, 0) * open_interest) DESC LIMIT 1) as call_wall,
            -- Put Wall: Strike with highest Put Net Gamma (Support) - updated to avoid deep ITM strikes
            (SELECT strike FROM LatestChain WHERE LOWER(option_type) = 'put' ORDER BY (IFNULL(gamma, 0) * open_interest) DESC LIMIT 1) as put_wall,
            -- Volatility Wall: Highest IV usually indicates fear or expected move
            (SELECT strike FROM LatestChain ORDER BY implied_volatility DESC LIMIT 1) as max_iv_strike
    ),
    TopFlows AS (
        SELECT 
            ARRAY_AGG(
                STRUCT(
                    option_type, 
                    strike, 
                    expiration_date, 
                    volume, 
                    open_interest, 
                    implied_volatility,
                    last_price
                ) ORDER BY volume DESC LIMIT 5
            ) as top_active_contracts
        FROM LatestChain
        WHERE dte BETWEEN 14 AND 60
    )
    SELECT
        s.*,
        w.*,
        f.top_active_contracts
    FROM SentimentStats s, Walls w, TopFlows f
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
    )
    
    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        if df.empty:
            return {}
            
        # Convert to a clean dict using pandas built-in JSON serialization
        # This handles numpy types (int64, float64, ndarray) automatically.
        import json
        data = json.loads(df.to_json(orient='records', date_format='iso'))[0]
        
        # Calculate a derived "Sentiment Label" for the LLM
        call_vol = data.get('total_call_vol', 0) or 0
        put_vol = data.get('total_put_vol', 0) or 0
        data['put_call_vol_ratio'] = round(put_vol / call_vol, 2) if call_vol > 0 else 0
        
        return data
    except Exception as e:
        logging.error(f"[{ticker}] Failed to fetch options market structure: {e}")
        return {}