import logging
import datetime
import pandas as pd
import pandas_ta as ta
from concurrent.futures import ProcessPoolExecutor, as_completed

from google.cloud import storage, bigquery
from config import (
    MAX_WORKERS,
    GCS_BUCKET_NAME,
    GCS_OUTPUT_FOLDER,
    INDICATORS,
    ROLLING_52_WEEK_WINDOW,
    BIGQUERY_TABLE_ID,
    CHUNK_SIZE,
)
from core.gcs import get_tickers, upload_json_to_gcs

def get_price_history_for_chunk(tickers: list[str], bq_client: bigquery.Client) -> pd.DataFrame:
    """Fetches OHLCV data for a chunk of tickers."""
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
    df["date"] = pd.to_datetime(df["date"])
    logging.info(f"Query complete. Found data for {df['ticker'].nunique()} unique tickers.")
    return df

def calculate_technicals_for_ticker(ticker: str, price_df: pd.DataFrame) -> dict:
    """
    Performs the CPU-bound technical analysis calculations for a single ticker.
    This function is safe to run in a separate process.
    """
    # 1. Data Cleaning
    core_columns = ['open', 'high', 'low', 'close', 'volume']
    for col in core_columns:
        price_df[col] = pd.to_numeric(price_df[col], errors='coerce')
    price_df[core_columns] = price_df[core_columns].ffill().bfill()
    price_df.dropna(subset=core_columns, inplace=True)
    if price_df.empty:
        return None

    # 2. Indicator Calculation
    try:
        strategy = ta.Strategy(
            name="ProfitScout Standard",
            ta=[{"kind": ind["kind"], **ind["params"]} for ind in INDICATORS.values()],
        )
        price_df.ta.strategy(strategy, append=True)
    except Exception as e:
        logging.warning(f"[{ticker}] SKIPPING ticker due to indicator calculation error: {e}")
        return None
        
    # 3. Final Computations & Formatting
    price_df["52w_high"] = price_df["high"].rolling(window=ROLLING_52_WEEK_WINDOW, min_periods=1).max()
    price_df["52w_low"] = price_df["low"].rolling(window=ROLLING_52_WEEK_WINDOW, min_periods=1).min()
    atr_col_name = f"ATRr_{INDICATORS['atr']['params']['length']}"
    if atr_col_name in price_df.columns:
        price_df["percent_atr"] = (price_df[atr_col_name] / price_df["close"]) * 100
    
    price_df.dropna(inplace=True)
    recent_df = price_df.tail(90).copy()
    if recent_df.empty:
        return None

    recent_df['date'] = recent_df['date'].dt.strftime('%Y-%m-%d')
    output_data = recent_df.to_dict(orient="records")

    return { 
        "ticker": ticker, 
        "as_of": datetime.date.today().isoformat(), 
        "technicals_timeseries": output_data 
    }

def run_pipeline(storage_client: storage.Client, bq_client: bigquery.Client):
    """
    Runs the full pipeline, processing tickers in parallel chunks using a ProcessPoolExecutor.
    """
    logging.info("--- Parallel Technicals Pipeline Started (ProcessPoolExecutor) ---")
    tickers = get_tickers(storage_client, GCS_BUCKET_NAME)
    if not tickers:
        logging.error("No tickers found in GCS. Exiting.")
        return

    logging.info(f"Found {len(tickers)} tickers. Starting parallel processing in chunks of {CHUNK_SIZE}.")

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i in range(0, len(tickers), CHUNK_SIZE):
            chunk = tickers[i:i + CHUNK_SIZE]
            logging.info(f"--- Processing chunk {i//CHUNK_SIZE + 1}/{-(-len(tickers)//CHUNK_SIZE)} ---")

            price_data_chunk = get_price_history_for_chunk(chunk, bq_client=bq_client)
            if price_data_chunk.empty:
                logging.warning("No price data returned for this chunk. Skipping.")
                continue

            grouped_by_ticker = {ticker: df for ticker, df in price_data_chunk.groupby("ticker")}
            
            futures = {
                executor.submit(calculate_technicals_for_ticker, ticker, price_df.copy()): ticker
                for ticker, price_df in grouped_by_ticker.items()
            }

            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    output_doc = future.result()
                    if output_doc:
                        blob_path = f"{GCS_OUTPUT_FOLDER}{ticker}_technicals.json"
                        upload_json_to_gcs(storage_client, GCS_BUCKET_NAME, output_doc, blob_path)
                        logging.info(f"[{ticker}] Successfully calculated and uploaded.")
                    else:
                        logging.info(f"[{ticker}] Calculation returned no data. Skipping upload.")
                except Exception as e:
                    logging.error(f"[{ticker}] A future completed with an unhandled exception: {e}", exc_info=True)
    
    logging.info("--- Technicals collection pipeline has finished all tasks. ---")