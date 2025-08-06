# ingestion/core/bq.py
import logging
import datetime
import pandas as pd
from google.cloud import bigquery
from . import config

def get_start_dates_for_populator(client: bigquery.Client, tickers: list[str]) -> dict:
    """Gets the next start date for each ticker from BigQuery for the populator."""
    query = f"""
        SELECT ticker, MAX(date) as max_date
        FROM `{config.PRICE_DATA_TABLE_ID}`
        WHERE ticker IN UNNEST(@tickers)
        GROUP BY ticker
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tickers", "STRING", tickers)]
    )
    try:
        results = client.query(query, job_config=job_config).result()
        max_dates = {row["ticker"]: row["max_date"] for row in results}
    except Exception as e:
        logging.error(f"Could not query BigQuery for max dates: {e}")
        max_dates = {}

    return {
        ticker: max_dates.get(ticker, config.DEFAULT_START_DATE - datetime.timedelta(days=1)) + datetime.timedelta(days=1)
        for ticker in tickers
    }

def load_data_to_bigquery(client: bigquery.Client, df: pd.DataFrame) -> int:
    """Loads a DataFrame into the target BigQuery price data table."""
    if df.empty:
        return 0
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    try:
        load_job = client.load_table_from_dataframe(df, config.PRICE_DATA_TABLE_ID, job_config=job_config)
        load_job.result()
        logging.info(f"Loaded {load_job.output_rows} rows into BigQuery.")
        return load_job.output_rows
    except Exception as e:
        logging.error(f"BigQuery load job failed: {e}")
        return 0