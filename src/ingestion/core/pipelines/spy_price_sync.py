# ingestion/core/pipelines/spy_price_sync.py
import datetime
import logging
import pandas as pd
from google.cloud import bigquery
from .. import config
from ..clients.fmp_client import FMPClient

def _load_spy_prices(fmp_client: FMPClient, start: datetime.date, end: datetime.date) -> pd.DataFrame:
    """Fetches SPY price history between start and end dates."""
    # Fetch prices. Note: 'SPY' is hardcoded as the ticker of interest.
    df = fmp_client.fetch_prices_for_populator("SPY", start, end)
    if df.empty:
        logging.warning("No SPY price data returned from provider.")
        return pd.DataFrame()
    df = df.sort_values("date")
    return df

def _load_to_bigquery(bq_client: bigquery.Client, df: pd.DataFrame) -> int:
    """Loads the SPY price data to BigQuery, replacing the table contents."""
    if df.empty:
        logging.info("Skipping BigQuery load because dataframe is empty.")
        return 0

    # We TRUNCATE (replace) the table every run to ensure a clean history without dupes.
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    
    try:
        load_job = bq_client.load_table_from_dataframe(
            df, config.SPY_PRICE_TABLE_ID, job_config=job_config
        )
        load_job.result()
        logging.info(f"Loaded {load_job.output_rows} SPY price rows into {config.SPY_PRICE_TABLE_ID}.")
        return load_job.output_rows or 0
    except Exception as e:
        logging.error(f"Failed to load SPY prices to BigQuery: {e}")
        raise

def run_pipeline(bq_client: bigquery.Client, fmp_client: FMPClient):
    """Fetches SPY prices and syncs them to BigQuery."""
    start_date = config.SPY_DEFAULT_START_DATE
    end_date = datetime.date.today()

    logging.info(f"Starting SPY price sync from {start_date} to {end_date}.")
    
    df = _load_spy_prices(fmp_client, start_date, end_date)
    if df.empty:
        logging.warning("SPY price sync aborted because no data was retrieved.")
        return

    bq_rows = _load_to_bigquery(bq_client, df)
    logging.info(f"SPY price ingestion complete. Rows loaded: {bq_rows}.")