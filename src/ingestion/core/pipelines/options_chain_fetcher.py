# ingestion/core/pipelines/options_chain_fetcher.py
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import pandas as pd
from google.cloud import bigquery, storage

from .. import config
from ..clients.polygon_client import PolygonClient

OPTIONS_TABLE = f"{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.options_chain"
PRICE_TABLE_ID = f"{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.price_data"
# --- UPDATED: Point to the correct bucket and file ---
TICKER_LIST_BUCKET = "profit-scout-data"
TICKER_LIST_PATH = "tickerlist.txt"


def _truncate_options_chain(bq_client: bigquery.Client):
    """Remove ALL previous rows so only today's snapshot remains, with retries."""
    q = f"TRUNCATE TABLE `{OPTIONS_TABLE}`"
    for attempt in range(3):
        try:
            job = bq_client.query(q)
            job.result()
            break
        except Exception:
            if attempt == 2:
                raise
            time.sleep(2**attempt)
    for _ in range(10):
        try:
            bq_client.query(f"SELECT 1 FROM `{OPTIONS_TABLE}` LIMIT 0").result()
            logging.info("Truncated %s and verified availability.", OPTIONS_TABLE)
            return
        except Exception:
            time.sleep(0.5)
    logging.warning(
        "Truncated %s but could not quickly verify availability.", OPTIONS_TABLE
    )


def _get_all_tickers(storage_client: storage.Client) -> pd.DataFrame:
    """
    Select ALL tickers from the tickerlist.txt file in GCS.
    """
    try:
        bucket = storage_client.bucket(TICKER_LIST_BUCKET)
        blob = bucket.blob(TICKER_LIST_PATH)
        content = blob.download_as_text(encoding="utf-8")
        tickers = [
            line.strip().upper() for line in content.splitlines() if line.strip()
        ]
        logging.info("Found %d tickers from %s.", len(tickers), TICKER_LIST_PATH)
        return pd.DataFrame(tickers, columns=["ticker"])
    except Exception as e:
        logging.error(f"Failed to load tickers from GCS: {e}", exc_info=True)
        return pd.DataFrame()


def _coerce_and_align(df: pd.DataFrame, ticker: str, today: date) -> pd.DataFrame:
    """
    Ensures the DataFrame matches the options_chain schema and types.
    """
    rename = {
        "contract_symbol": "contract_symbol",
        "option_type": "option_type",
        "expiration_date": "expiration_date",
        "strike": "strike",
        "last_price": "last_price",
        "bid": "bid",
        "ask": "ask",
        "volume": "volume",
        "open_interest": "open_interest",
        "implied_volatility": "implied_volatility",
        "delta": "delta",
        "theta": "theta",
        "vega": "vega",
        "gamma": "gamma",
        "underlying_price": "underlying_price",
    }
    df = df.rename(columns=rename)
    if "option_type" in df.columns:
        df["option_type"] = df["option_type"].astype(str).str.lower()
    df["ticker"] = ticker
    df["fetch_date"] = today
    num_cols = [
        "strike",
        "last_price",
        "bid",
        "ask",
        "volume",
        "open_interest",
        "implied_volatility",
        "delta",
        "theta",
        "vega",
        "gamma",
        "underlying_price",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # --- THIS IS THE FIX ---
    # A more robust method to prevent any timezone-related date shifts.
    # It extracts the 'YYYY-MM-DD' from the string and converts it directly,
    # ignoring any problematic time or timezone information.
    if "expiration_date" in df.columns:
        date_strings = df["expiration_date"].astype(str).str.slice(0, 10)
        datetimes = pd.to_datetime(date_strings, format="%Y-%m-%d", errors="coerce")
        df["expiration_date"] = datetimes.dt.date

    df = df.dropna(subset=["expiration_date", "contract_symbol"])
    df["dte"] = (
        pd.to_datetime(df["expiration_date"]) - pd.to_datetime(today)
    ).dt.days.astype("Int64")
    df = df[df["dte"] >= 0]
    cols = [
        "ticker",
        "contract_symbol",
        "option_type",
        "expiration_date",
        "strike",
        "last_price",
        "bid",
        "ask",
        "volume",
        "open_interest",
        "implied_volatility",
        "delta",
        "theta",
        "vega",
        "gamma",
        "underlying_price",
        "fetch_date",
        "dte",
    ]
    return df.reindex(columns=cols)


def _fill_underlying_from_bq_if_needed(
    bq_client: bigquery.Client, df: pd.DataFrame, ticker: str
) -> pd.DataFrame:
    """
    If any missing underlying_price values remain, fill from latest adj_close.
    """
    if df.empty or not df["underlying_price"].isna().any():
        return df
    q = f"SELECT adj_close FROM `{PRICE_TABLE_ID}` WHERE ticker = @ticker ORDER BY date DESC LIMIT 1"
    job = bq_client.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
        ),
    )
    rows = list(job)
    if not rows or rows[0]["adj_close"] is None:
        logging.warning("[%s] No fallback adj_close in %s", ticker, PRICE_TABLE_ID)
        return df
    fallback = float(rows[0]["adj_close"])
    df["underlying_price"] = df["underlying_price"].fillna(fallback)
    logging.info(
        "[%s] Filled missing underlying_price from price_data: %.4f", ticker, fallback
    )
    return df


def _fetch_and_load_chain_for_ticker(
    client: PolygonClient, bq_client: bigquery.Client, ticker: str
):
    """
    Fetch Polygon option chain (≤90d) for ticker, then append to BigQuery.
    """
    today = date.today()
    logging.info("[%s] Fetching Polygon chain (≤90d).", ticker)
    raw = client.fetch_options_chain(ticker, max_days=90)
    if not raw:
        logging.warning("[%s] No contracts returned.", ticker)
        return
    df = _coerce_and_align(pd.DataFrame(raw), ticker, today)
    df = _fill_underlying_from_bq_if_needed(bq_client, df, ticker)
    if df.empty:
        logging.info("[%s] No contracts to load.", ticker)
        return
    logging.info("[%s] Loading %d contracts into %s", ticker, len(df), OPTIONS_TABLE)
    job = bq_client.load_table_from_dataframe(
        df,
        OPTIONS_TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()


def run_pipeline(
    polygon_client: PolygonClient | None = None,
    bq_client: bigquery.Client | None = None,
):
    """
    Main entry: Truncates table, gets all tickers from GCS, and fetches/loads chains.
    """
    logging.info("--- Starting Options Chain Fetcher (Polygon) ---")
    bq_client = bq_client or bigquery.Client(project=config.PROJECT_ID)
    polygon_client = polygon_client or PolygonClient(api_key=config.POLYGON_API_KEY)
    storage_client = storage.Client(project=config.PROJECT_ID)

    _truncate_options_chain(bq_client)

    work = _get_all_tickers(storage_client)
    if work.empty:
        logging.warning("No tickers identified. Exiting.")
        return

    tickers = ", ".join(sorted(set(work["ticker"].tolist())))
    logging.info("Tickers selected (%d): %s", len(work), tickers)

    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = {
            ex.submit(
                _fetch_and_load_chain_for_ticker, polygon_client, bq_client, r.ticker
            ): r.ticker
            for _, r in work.iterrows()
        }
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                logging.error("[%s] Worker failed: %s", futures[fut], e, exc_info=True)

    logging.info("--- Options Chain Fetcher Finished ---")
