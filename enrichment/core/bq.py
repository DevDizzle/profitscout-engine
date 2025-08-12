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