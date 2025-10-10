# serving/core/pipelines/sync_options_to_firestore.py
import logging
import pandas as pd
from google.cloud import firestore, bigquery
from .. import config
import numpy as np

# --- Configuration Updated ---
BATCH_SIZE = 500
# POINT TO THE NEW SIGNALS TABLE
SIGNALS_TABLE_ID = (
    f"{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_analysis_signals"
)
# RENAME THE COLLECTION FOR CLARITY
FIRESTORE_COLLECTION_NAME = "options_signals"


def _iter_batches(iterable, n):
    """Yield successive n-sized chunks from iterable."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch


def _commit_ops(db, ops):
    """Commits a list of Firestore operations in batches."""
    batch = db.batch()
    count = 0
    for op in ops:
        if op["type"] == "set":
            batch.set(op["ref"], op["data"])
        elif op["type"] == "delete":
            batch.delete(op["ref"])
        count += 1
        if count >= BATCH_SIZE:
            batch.commit()
            batch = db.batch()
            count = 0
    if count:
        batch.commit()


def _delete_collection_in_batches(collection_ref):
    """Wipes all documents from a Firestore collection."""
    logging.info(f"Wiping Firestore collection: '{collection_ref.id}'...")
    deleted_count = 0
    while True:
        docs = list(collection_ref.limit(BATCH_SIZE).stream())
        if not docs:
            break
        ops = [{"type": "delete", "ref": d.reference} for d in docs]
        _commit_ops(firestore.Client(project=config.DESTINATION_PROJECT_ID), ops)
        deleted_count += len(ops)
        logging.info(f"Deleted {deleted_count} docs...")
    logging.info(f"Wipe complete for collection '{collection_ref.id}'.")


def _load_bq_df(bq: bigquery.Client, query: str) -> pd.DataFrame:
    """Loads data from a BigQuery query into a pandas DataFrame and cleans it."""
    df = bq.query(query).to_dataframe()
    if not df.empty:
        # Convert any remaining date/time columns to string for Firestore
        for col in df.columns:
            dtype_str = str(df[col].dtype)
            if "datetime" in dtype_str or "dbdate" in dtype_str:
                df[col] = df[col].astype(str)

        df = df.replace({pd.NA: np.nan})
        df = df.where(pd.notna(df), None)
    return df


def run_pipeline(full_reset: bool = False):
    """
    Syncs the processed options signals from BigQuery to Firestore,
    structuring them for the web application.
    """
    db = firestore.Client(project=config.DESTINATION_PROJECT_ID)
    bq = bigquery.Client(project=config.SOURCE_PROJECT_ID)

    collection_ref = db.collection(FIRESTORE_COLLECTION_NAME)
    logging.info(f"--- Options Signals Firestore Sync Pipeline ---")
    logging.info(f"Target collection: {collection_ref.id}")
    logging.info(f"Full reset? {'YES' if full_reset else 'NO'}")

    try:
        # --- THIS IS THE FIX ---
        # The query now explicitly casts the date fields to STRING to avoid conversion issues.
        signals_query = f"""
            WITH LatestMetadata AS (
                SELECT 
                    ticker,
                    company_name,
                    ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY quarter_end_date DESC) as rn
                FROM `{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.stock_metadata`
            )
            SELECT
                t1.ticker,
                CAST(t1.run_date AS STRING) AS run_date,
                CAST(t1.expiration_date AS STRING) AS expiration_date,
                t1.strike_price,
                t1.implied_volatility,
                t1.iv_signal,
                t1.stock_price_trend_signal,
                t1.setup_quality_signal,
                t1.summary,
                t1.contract_symbol,
                t1.option_type,
                t2.company_name
            FROM `{SIGNALS_TABLE_ID}` AS t1
            LEFT JOIN LatestMetadata AS t2 ON t1.ticker = t2.ticker AND t2.rn = 1
        """
        signals_df = _load_bq_df(bq, signals_query)
    except Exception as e:
        logging.critical(
            f"Failed to query options signals from BigQuery: {e}", exc_info=True
        )
        raise

    if full_reset:
        _delete_collection_in_batches(collection_ref)

    if signals_df.empty:
        logging.warning(
            "No options signals found in BigQuery. Collection will be empty or unchanged."
        )
        return

    upsert_ops = []
    # NEW LOGIC: Group by ticker and then create separate lists for calls and puts
    for ticker, group in signals_df.groupby("ticker"):
        doc_ref = collection_ref.document(ticker)

        # Separate calls and puts
        calls = group[group["option_type"] == "call"].to_dict("records")
        puts = group[group["option_type"] == "put"].to_dict("records")

        # Find the most recent company_name, handling potential nulls
        company_name = (
            group["company_name"].dropna().iloc[0]
            if not group["company_name"].dropna().empty
            else ticker
        )

        data = {
            "ticker": ticker,
            "company_name": company_name,
            "calls": calls,
            "puts": puts,
        }
        upsert_ops.append({"type": "set", "ref": doc_ref, "data": data})

    logging.info(
        f"Upserting {len(upsert_ops)} ticker documents to '{collection_ref.id}'..."
    )
    for chunk in _iter_batches(upsert_ops, BATCH_SIZE):
        _commit_ops(db, chunk)

    # Prune any tickers that no longer have signals
    current_tickers = set(signals_df["ticker"].unique())
    existing_tickers_docs = list(collection_ref.stream())
    existing_tickers = [doc.id for doc in existing_tickers_docs]
    to_delete = [k for k in existing_tickers if k not in current_tickers]

    if to_delete:
        logging.info(
            f"Deleting {len(to_delete)} stale ticker documents from '{collection_ref.id}'..."
        )
        delete_ops = [
            {"type": "delete", "ref": collection_ref.document(k)} for k in to_delete
        ]
        for chunk in _iter_batches(delete_ops, BATCH_SIZE):
            _commit_ops(db, chunk)

    logging.info(f"Sync complete for '{collection_ref.id}'.")
    logging.info("--- Options Signals Firestore Sync Pipeline Finished ---")
