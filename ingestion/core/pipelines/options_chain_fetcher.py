# ingestion/core/pipelines/options_chain_fetcher.py
import logging
import pandas as pd
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from .. import config
from ..clients.polygon_client import PolygonClient

OPTIONS_TABLE = f"{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.options_chain"


def _truncate_options_chain(bq_client: bigquery.Client):
    """Remove ALL previous rows so only today's snapshot remains."""
    q = f"TRUNCATE TABLE `{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.options_chain`"
    job = bq_client.query(q)
    job.result()
    logging.info("Truncated %s", OPTIONS_TABLE)


def _get_buy_universe(client: bigquery.Client) -> pd.DataFrame:
    """
    Selects ALL tickers for the most recent run_date where weighted_score > 0.62.
    """
    q = f"""
    WITH latest_run AS (
      SELECT MAX(run_date) AS run_date
      FROM `{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.analysis_scores`
      WHERE weighted_score IS NOT NULL
    )
    SELECT
      s.ticker,
      'BUY' AS signal,
      s.weighted_score,
      s.run_date
    FROM `{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.analysis_scores` AS s
    JOIN latest_run r
      ON s.run_date = r.run_date
    WHERE s.weighted_score > 0.62
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.ticker ORDER BY s.weighted_score DESC) = 1
    """
    df = client.query(q).to_dataframe()
    if not df.empty:
        used = df["run_date"].iloc[0]
        logging.info(
            "Using latest run_date=%s; selected %d tickers with weighted_score > 0.62",
            used, len(df),
        )
    else:
        logging.warning("No tickers above threshold found for latest run_date.")
    return df


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

    # Normalize option_type to lowercase (call/put)
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

    df["expiration_date"] = pd.to_datetime(df["expiration_date"], errors="coerce").dt.date
    df = df.dropna(subset=["expiration_date", "contract_symbol"])

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
    ]
    return df.reindex(columns=cols)


def _fetch_and_load_chain_for_ticker(
    client: PolygonClient, bq_client: bigquery.Client, ticker: str, signal: str
):
    """
    Fetch Polygon option chain (≤90d) for ticker, keep CALLs only (BUY universe),
    then append to BigQuery.
    """
    today = date.today()
    logging.info("[%s] Fetching Polygon chain (≤90d).", ticker)
    raw = client.fetch_options_chain(ticker, max_days=90)
    if not raw:
        logging.warning("[%s] No contracts returned.", ticker)
        return

    df = _coerce_and_align(pd.DataFrame(raw), ticker, today)

    # Only BUY universe => CALLs
    desired = "call"
    before = len(df)
    df = df[df["option_type"] == desired]
    logging.info("[%s] Direction=%s; kept %d/%d contracts.", ticker, desired.upper(), len(df), before)

    if df.empty:
        logging.info("[%s] Nothing to load after direction filter.", ticker)
        return

    logging.info("[%s] Loading %d contracts into %s", ticker, len(df), OPTIONS_TABLE)
    job = bq_client.load_table_from_dataframe(
        df, OPTIONS_TABLE, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()


def run_pipeline(polygon_client: PolygonClient | None = None, bq_client: bigquery.Client | None = None):
    """
    Main entry: TRUNCATE table, select latest-run tickers with weighted_score > 0.62,
    fetch Polygon chains (≤90d), keep CALLs only, and load today's rows.
    """
    logging.info("--- Starting Options Chain Fetcher (Polygon) ---")
    bq_client = bq_client or bigquery.Client(project=config.PROJECT_ID)
    polygon_client = polygon_client or PolygonClient(api_key=config.POLYGON_API_KEY)

    # Remove ALL previous rows; we only keep today's snapshot.
    _truncate_options_chain(bq_client)

    work = _get_buy_universe(bq_client)
    if work.empty:
        logging.warning("No tickers identified. Exiting.")
        return

    tickers = ", ".join(sorted(set(work["ticker"].tolist())))
    logging.info("Tickers selected (%d): %s", len(work), tickers)

    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = {
            ex.submit(_fetch_and_load_chain_for_ticker, polygon_client, bq_client, r.ticker, r.signal): r.ticker
            for _, r in work.iterrows()
        }
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                logging.error("[%s] Worker failed: %s", futures[fut], e, exc_info=True)

    logging.info("--- Options Chain Fetcher Finished ---")
