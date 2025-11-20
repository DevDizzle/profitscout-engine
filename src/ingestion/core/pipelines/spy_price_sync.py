# ingestion/core/pipelines/spy_price_sync.py
import datetime
import logging
from typing import Iterable

import pandas as pd
from google.cloud import bigquery, firestore

from .. import config
from ..clients.fmp_client import FMPClient


def _load_spy_prices(fmp_client: FMPClient, start: datetime.date, end: datetime.date) -> pd.DataFrame:
    """Fetches SPY price history between start and end dates."""
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
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    load_job = bq_client.load_table_from_dataframe(df, config.SPY_PRICE_TABLE_ID, job_config=job_config)
    load_job.result()
    logging.info(f"Loaded {load_job.output_rows} SPY price rows into {config.SPY_PRICE_TABLE_ID}.")
    return load_job.output_rows or 0


def _batched(iterable: Iterable, size: int):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _sync_to_firestore(firestore_client: firestore.Client, df: pd.DataFrame):
    """Syncs the SPY price dataframe to Firestore using date as the document id."""
    if df.empty:
        logging.info("No SPY price data to sync to Firestore.")
        return 0

    collection = firestore_client.collection(config.SPY_PRICE_FIRESTORE_COLLECTION)
    docs = df.copy()
    docs["date"] = docs["date"].apply(lambda d: d.isoformat() if isinstance(d, datetime.date) else str(d))
    records = docs.to_dict(orient="records")

    total_written = 0
    for chunk in _batched(records, 500):
        batch = firestore_client.batch()
        for doc in chunk:
            doc_id = doc["date"]
            batch.set(collection.document(doc_id), doc)
        batch.commit()
        total_written += len(chunk)
    logging.info(f"Synced {total_written} SPY price documents to Firestore collection '{config.SPY_PRICE_FIRESTORE_COLLECTION}'.")
    return total_written


def run_pipeline(bq_client: bigquery.Client, firestore_client: firestore.Client, fmp_client: FMPClient):
    """Fetches SPY prices since the configured start date and syncs them to BigQuery and Firestore."""
    start_date = config.SPY_DEFAULT_START_DATE
    end_date = datetime.date.today()

    logging.info(f"Starting SPY price sync from {start_date} to {end_date}.")
    df = _load_spy_prices(fmp_client, start_date, end_date)
    if df.empty:
        logging.warning("SPY price sync aborted because no data was retrieved.")
        return {"bq_rows": 0, "firestore_docs": 0}

    bq_rows = _load_to_bigquery(bq_client, df)
    firestore_docs = _sync_to_firestore(firestore_client, df)

    logging.info(
        f"SPY price sync complete. BigQuery rows loaded: {bq_rows}. Firestore documents written: {firestore_docs}."
    )
    return {"bq_rows": bq_rows, "firestore_docs": firestore_docs}
