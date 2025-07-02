# technicals_collector/core/orchestrator.py
import logging
import datetime
import pandas as pd
import pandas_ta as ta
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

from google.cloud import storage, bigquery
from config import (
    MAX_WORKERS,
    GCS_BUCKET_NAME,
    GCS_OUTPUT_FOLDER,
    INDICATORS,
    ROLLING_52_WEEK_WINDOW,
    BIGQUERY_TABLE_ID,
)
from core.gcs import get_tickers, upload_json_to_gcs

# The BigQuery client is created in main.py and passed to these functions.
def get_all_price_histories(tickers: list[str], bq_client: bigquery.Client) -> pd.DataFrame:
    """
    Fetches OHLCV data for ALL tickers in a single, efficient BigQuery query.
    """
    if not bq_client:
        raise ConnectionError("BigQuery client is not initialized.")
    if not tickers:
        return pd.DataFrame()

    logging.info(f"Querying BigQuery for price history of {len(tickers)} tickers...")
    
    query = f"""
        SELECT ticker, date, open, high, low, adj_close as close, volume
        FROM `{BIGQUERY_TABLE_ID}`
        WHERE ticker IN UNNEST(@tickers)
        AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL {ROLLING_52_WEEK_WINDOW + 100} DAY)
        ORDER BY ticker, date ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tickers", "STRING", tickers)]
    )
    df = bq_client.query(query, job_config=job_config).to_dataframe()

    if df.empty:
        logging.warning("BigQuery query returned no price data for the given tickers.")
    else:
        logging.info(f"BigQuery query complete. Found data for {df['ticker'].nunique()} unique tickers.")

    df["date"] = pd.to_datetime(df["date"])
    return df

def process_ticker_data(ticker: str, price_df: pd.DataFrame, storage_client: storage.Client) -> str:
    """
    Cleans data, calculates indicators, trims the history, formats to a clean 
    time-series, and uploads the result for a single ticker.
    """
    try:
        logging.info(f"[{ticker}] Starting data processing.")
        
        core_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in core_columns:
            price_df[col] = pd.to_numeric(price_df[col], errors='coerce')
        price_df[core_columns] = price_df[core_columns].ffill().bfill()
        price_df.dropna(subset=core_columns, inplace=True)

        if price_df.empty:
            return f"[{ticker}] Skipped: No valid price data after cleaning."
        
        try:
            strategy = ta.Strategy(
                name="ProfitScout Standard",
                ta=[{"kind": ind["kind"], **ind["params"]} for ind in INDICATORS.values()],
            )
            price_df.ta.strategy(strategy, append=True)
            logging.info(f"[{ticker}] Technical indicators calculated.")
        except Exception as e:
            logging.warning(f"[{ticker}] SKIPPING ticker due to indicator calculation error: {e}")
            return f"[{ticker}] Skipped due to indicator calculation error."

        price_df["52w_high"] = price_df["high"].rolling(window=ROLLING_52_WEEK_WINDOW, min_periods=1).max()
        price_df["52w_low"] = price_df["low"].rolling(window=ROLLING_52_WEEK_WINDOW, min_periods=1).min()
        atr_col_name = f"ATRr_{INDICATORS['atr']['params']['length']}"
        if atr_col_name in price_df.columns:
            price_df["percent_atr"] = (price_df[atr_col_name] / price_df["close"]) * 100

        price_df.dropna(inplace=True)
        recent_df = price_df.tail(90).copy()
        
        if recent_df.empty:
            return f"[{ticker}] Skipped: No data left after cleaning and trimming."

        recent_df['date'] = recent_df['date'].dt.strftime('%Y-%m-%d')
        output_data = recent_df.to_dict(orient="records")

        output_doc = { 
            "ticker": ticker, 
            "as_of": datetime.date.today().isoformat(), 
            "technicals_timeseries": output_data 
        }
        
        blob_path = f"{GCS_OUTPUT_FOLDER}{ticker}_technicals.json"
        
        upload_json_to_gcs(storage_client, GCS_BUCKET_NAME, output_doc, blob_path)
        return f"[{ticker}] Technicals JSON uploaded successfully (Refactored)."

    except Exception as e:
        logging.error(f"[{ticker}] An unexpected error occurred during data processing: {e}", exc_info=True)
        return f"[{ticker}] Failed with error: {e}"


def run_pipeline(storage_client: storage.Client, bq_client: bigquery.Client):
    """
    Runs the full technicals pipeline with timeouts to prevent hanging.
    """
    logging.info("--- Technicals Pipeline Orchestrator: run_pipeline initiated. ---")
    if not bq_client:
        logging.error("BigQuery client not initialized. Aborting pipeline.")
        return

    tickers = get_tickers(storage_client, GCS_BUCKET_NAME)
    if not tickers:
        logging.error("No tickers found in GCS. Exiting.")
        return
    logging.info(f"Found {len(tickers)} tickers in the source file.")

    all_price_data = get_all_price_histories(tickers, bq_client=bq_client)
    if all_price_data.empty:
        logging.warning("Pipeline halting as no price data was returned from BigQuery.")
        return

    grouped_by_ticker = {ticker: df for ticker, df in all_price_data.groupby("ticker")}
    logging.info(f"Grouped data for {len(grouped_by_ticker)} tickers to prepare for processing.")

    logging.info(f"Starting ThreadPoolExecutor with a max of {MAX_WORKERS} workers.")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_ticker_data, ticker, price_df.copy(), storage_client): ticker
            for ticker, price_df in grouped_by_ticker.items()
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result(timeout=120)
                logging.info(f"TASK_COMPLETE: {result}")
            except TimeoutError:
                logging.error(f"[{ticker}] Processing timed out after 120 seconds and was skipped.")
            except Exception as e:
                logging.error(f"[{ticker}] A future completed with an unhandled exception: {e}", exc_info=True)

    logging.info("--- Technicals collection pipeline has finished all tasks. ---")