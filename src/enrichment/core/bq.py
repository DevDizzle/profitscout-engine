# enrichment/core/bq.py
import logging
import pandas as pd
from google.cloud import bigquery
from . import config

def get_latest_transcript_work_list() -> pd.DataFrame:
    """
    Queries the master metadata table to get the latest transcript record for each ticker.
    """
    client = bigquery.Client()
    logging.info(f"Querying BigQuery table: {config.BQ_METADATA_TABLE}")
    
    # This query finds the single most recent entry for each ticker
    query = f"""
        SELECT
            ticker,
            CAST(quarter_end_date AS STRING) AS date_str
        FROM (
            SELECT
                ticker,
                quarter_end_date,
                ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY quarter_end_date DESC) as rn
            FROM
                `{config.BQ_METADATA_TABLE}`
        )
        WHERE rn = 1
    """
    try:
        df = client.query(query).to_dataframe()
        logging.info(f"Successfully fetched {len(df)} tickers for transcript analysis.")
        return df
    except Exception as e:
        logging.critical(f"Failed to query BigQuery for the work list: {e}", exc_info=True)
        return pd.DataFrame()

def load_df_to_bq(df: pd.DataFrame, table_id: str, project_id: str, write_disposition: str = "WRITE_TRUNCATE"):
    """
    Loads a pandas DataFrame into a BigQuery table using simple APPEND or TRUNCATE.
    This is used by the score_aggregator.
    """
    if df.empty:
        logging.warning("DataFrame is empty. Skipping BigQuery load.")
        return
    
    client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ],
    )
    
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logging.info(f"Loaded {job.output_rows} rows into BigQuery table: {table_id} using {write_disposition}")
    except Exception as e:
        logging.error(f"Failed to load DataFrame to {table_id}: {e}", exc_info=True)
        raise