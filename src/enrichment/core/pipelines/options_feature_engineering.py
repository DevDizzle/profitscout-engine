# enrichment/core/pipelines/options_feature_engineering.py
import logging
import pandas as pd
import numpy as np
from datetime import date
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from .. import config
from .. import options_analysis_helper as helper

logging.basicConfig(level=logging.INFO)


def _get_candidate_tickers(bq_client: bigquery.Client) -> list[str]:
    """
    Gets the list of unique tickers from the most recent run of the options_candidates table.
    """
    logging.info("Fetching tickers from the latest options_candidates run...")
    query = f"""
        SELECT DISTINCT ticker
        FROM `{config.CAND_TABLE}`
        WHERE selection_run_ts = (SELECT MAX(selection_run_ts) FROM `{config.CAND_TABLE}`)
    """
    try:
        df = bq_client.query(query).to_dataframe()
        tickers = df["ticker"].unique().tolist()
        logging.info(f"Found {len(tickers)} unique tickers in options_candidates.")
        return tickers
    except Exception as e:
        logging.error(
            f"Failed to load tickers from options_candidates: {e}", exc_info=True
        )
        return []


def _fetch_all_data(
    tickers: list[str], bq_client: bigquery.Client
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetches all options chain and price data for the candidate tickers.
    """
    if not tickers:
        return pd.DataFrame(), pd.DataFrame()
    logging.info(f"Fetching bulk data for {len(tickers)} candidate tickers...")

    chain_query = f"""
        SELECT *
        FROM `{config.CHAIN_TABLE}`
        WHERE ticker IN UNNEST(@tickers) AND fetch_date = CURRENT_DATE()
    """

    price_history_query = f"""
        SELECT ticker, date, open, high, low, adj_close AS close, volume
        FROM `{config.PRICE_TABLE_ID}`
        WHERE ticker IN UNNEST(@tickers) AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tickers", "STRING", tickers)]
    )

    chain_df = bq_client.query(chain_query, job_config=job_config).to_dataframe()
    price_history_df = bq_client.query(
        price_history_query, job_config=job_config
    ).to_dataframe()

    logging.info(
        f"Fetched {len(chain_df)} chain records and {len(price_history_df)} price records."
    )
    return chain_df, price_history_df


def _process_ticker(
    ticker: str, chain_df: pd.DataFrame, price_history_df: pd.DataFrame
) -> Optional[dict]:
    """
    Worker function to process data for one ticker.
    Now calculates Total Net Gamma Exposure (GEX).
    """
    try:
        if price_history_df.empty:
            logging.warning(
                f"[{ticker}] No price history data found. Cannot generate features."
            )
            return None

        latest_price_row = price_history_df.sort_values("date").iloc[-1]
        as_of_date = pd.to_datetime(latest_price_row["date"]).date()

        iv_avg, iv_signal, total_gex = None, None, None
        
        if not chain_df.empty:
            uprice = (
                chain_df["underlying_price"].dropna().iloc[0]
                if "underlying_price" in chain_df.columns
                and not chain_df["underlying_price"].dropna().empty
                else latest_price_row["close"]
            )
            
            # Calculate IV features
            iv_avg = helper.compute_iv_avg_atm(chain_df, uprice, as_of_date)
            hv_30_for_signal = helper.compute_hv30(
                None, ticker, as_of_date, price_history_df=price_history_df
            )
            if iv_avg is not None and hv_30_for_signal is not None:
                iv_signal = "high" if iv_avg > (hv_30_for_signal + 0.10) else "low"
                
            # Calculate Total Net Gamma Exposure using the helper
            if hasattr(helper, 'compute_net_gex'):
                total_gex = helper.compute_net_gex(chain_df, uprice)

        hv_30 = helper.compute_hv30(
            None, ticker, as_of_date, price_history_df=price_history_df
        )
        tech = helper.compute_technicals_and_deltas(price_history_df)

        return {
            "ticker": ticker,
            "date": as_of_date,
            "open": (
                float(latest_price_row["open"])
                if pd.notna(latest_price_row["open"])
                else None
            ),
            "high": (
                float(latest_price_row["high"])
                if pd.notna(latest_price_row["high"])
                else None
            ),
            "low": (
                float(latest_price_row["low"])
                if pd.notna(latest_price_row["low"])
                else None
            ),
            "adj_close": (
                float(latest_price_row["close"])
                if pd.notna(latest_price_row["close"])
                else None
            ),
            "volume": (
                int(latest_price_row["volume"])
                if pd.notna(latest_price_row["volume"])
                else None
            ),
            "iv_avg": iv_avg,
            "hv_30": hv_30,
            "iv_signal": iv_signal,
            "total_gex": total_gex,
            **tech,
        }
    except Exception as e:
        logging.error(f"Error processing ticker {ticker}: {e}", exc_info=True)
        return None


def _truncate_and_load_results(bq_client: bigquery.Client, df: pd.DataFrame):
    """
    Performs a full wipe-and-replace load into the target table using DataFrame.
    """
    if df.empty:
        logging.warning("No results to load. Skipping BigQuery load.")
        return

    # Define table ID
    table_id = f"{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.options_analysis_input"

    # Replace Inf/-Inf/NaN with None/NaN for valid JSON/Parquet conversion
    df_clean = df.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df), None)

    # Configure the load job
    # FIX: Removed schema_update_options because it conflicts with WRITE_TRUNCATE
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    try:
        # Use load_table_from_dataframe which handles NaN/None/Types natively
        job = bq_client.load_table_from_dataframe(df_clean, table_id, job_config=job_config)
        job.result()
        logging.info(
            f"Successfully truncated and loaded {job.output_rows} rows into {table_id}"
        )
    except Exception as e:
        logging.error(f"Failed to load data to {table_id}: {e}", exc_info=True)
        raise


def run_pipeline():
    """Main pipeline to run the options feature engineering."""
    logging.info(
        "--- Starting Options Feature Engineering Pipeline (Wipe and Replace Mode) ---"
    )
    bq_client = bigquery.Client(project=config.PROJECT_ID)

    tickers = _get_candidate_tickers(bq_client)
    if not tickers:
        logging.warning("No tickers found from options_candidates. Exiting.")
        # Ensure table exists via helper, or just let it be
        helper.ensure_table_exists(bq_client)
        return

    all_chains_df, all_prices_df = _fetch_all_data(tickers, bq_client)

    chains_by_ticker = {
        ticker: group for ticker, group in all_chains_df.groupby("ticker")
    }
    prices_by_ticker = {
        ticker: group.sort_values("date")
        for ticker, group in all_prices_df.groupby("ticker")
    }

    results = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        future_to_ticker = {
            executor.submit(
                _process_ticker,
                ticker,
                chains_by_ticker.get(ticker, pd.DataFrame()),
                prices_by_ticker.get(ticker, pd.DataFrame()),
            ): ticker
            for ticker in tickers
        }
        for future in as_completed(future_to_ticker):
            result = future.result()
            if result:
                results.append(result)

    if not results:
        logging.warning("No features were generated after processing.")
        return

    results_df = pd.DataFrame(results)
    
    _truncate_and_load_results(bq_client, results_df)

    logging.info("--- Options Feature Engineering Pipeline Finished ---")