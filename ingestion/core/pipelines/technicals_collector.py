# ingestion/core/orchestrators/technicals_collector.py
import logging
import datetime
import pandas as pd
import pandas_ta as ta
from concurrent.futures import ProcessPoolExecutor, as_completed
from google.cloud import storage, bigquery
from .. import config
from ..gcs import get_tickers, upload_json_to_gcs

def _get_price_history_for_chunk(tickers: list[str], bq_client: bigquery.Client) -> pd.DataFrame:
    """Fetches OHLCV data for a chunk of tickers."""
    logging.info(f"Querying BigQuery for price history of {len(tickers)} tickers...")
    query = f"""
        SELECT ticker, date, open, high, low, adj_close as close, volume
        FROM `{config.TECHNICALS_PRICE_TABLE_ID}`
        WHERE ticker IN UNNEST(@tickers)
        AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL {config.ROLLING_52_WEEK_WINDOW + 100} DAY)
        ORDER BY ticker, date ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tickers", "STRING", tickers)]
    )
    df = bq_client.query(query, job_config=job_config).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])
    logging.info(f"Query complete. Found data for {df['ticker'].nunique()} unique tickers.")
    return df

def _calculate_technicals_for_ticker(ticker: str, price_df: pd.DataFrame) -> dict:
    """Performs the CPU-bound technical analysis calculations for a single ticker."""
    core_columns = ['open', 'high', 'low', 'close', 'volume']
    for col in core_columns:
        price_df[col] = pd.to_numeric(price_df[col], errors='coerce')
    price_df[core_columns] = price_df[core_columns].ffill().bfill()
    price_df.dropna(subset=core_columns, inplace=True)
    if price_df.empty: return None

    try:
        strategy = ta.Strategy(
            name="ProfitScout Standard",
            ta=[{"kind": ind["kind"], **ind["params"]} for ind in config.INDICATORS.values()],
        )
        price_df.ta.strategy(strategy, append=True)
    except Exception as e:
        logging.warning(f"[{ticker}] SKIPPING due to indicator calculation error: {e}")
        return None
        
    price_df["52w_high"] = price_df["high"].rolling(window=config.ROLLING_52_WEEK_WINDOW, min_periods=1).max()
    price_df["52w_low"] = price_df["low"].rolling(window=config.ROLLING_52_WEEK_WINDOW, min_periods=1).min()
    atr_col_name = f"ATRr_{config.INDICATORS['atr']['params']['length']}"
    if atr_col_name in price_df.columns:
        price_df["percent_atr"] = (price_df[atr_col_name] / price_df["close"]) * 100
    
    price_df.dropna(inplace=True)
    recent_df = price_df.tail(90).copy()
    if recent_df.empty: return None

    recent_df['date'] = recent_df['date'].dt.strftime('%Y-%m-%d')
    output_data = recent_df.to_dict(orient="records")
    return {"ticker": ticker, "as_of": datetime.date.today().isoformat(), "technicals_timeseries": output_data}

def run_pipeline(storage_client: storage.Client, bq_client: bigquery.Client):
    """Runs the full pipeline, processing tickers in parallel chunks."""
    logging.info("--- Parallel Technicals Pipeline Started ---")
    tickers = get_tickers(storage_client)
    if not tickers:
        logging.error("No tickers found. Exiting technicals pipeline.")
        return

    max_workers = config.MAX_WORKERS_TIERING.get("technicals_collector")
    chunk_size = config.BATCH_SIZE
    logging.info(f"Processing {len(tickers)} tickers in chunks of {chunk_size}.")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for i in range(0, len(tickers), chunk_size):
            chunk = tickers[i:i + chunk_size]
            price_data_chunk = _get_price_history_for_chunk(chunk, bq_client=bq_client)
            if price_data_chunk.empty:
                logging.warning("No price data for this chunk. Skipping.")
                continue

            grouped_by_ticker = {ticker: df for ticker, df in price_data_chunk.groupby("ticker")}
            futures = {executor.submit(_calculate_technicals_for_ticker, ticker, df.copy()): ticker for ticker, df in grouped_by_ticker.items()}

            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    if output_doc := future.result():
                        blob_path = f"{config.TECHNICALS_OUTPUT_FOLDER}{ticker}_technicals.json"
                        upload_json_to_gcs(storage_client, output_doc, blob_path)
                        logging.info(f"[{ticker}] Successfully calculated and uploaded.")
                except Exception as e:
                    logging.error(f"[{ticker}] A future failed with an exception: {e}", exc_info=True)
    
    logging.info("--- Technicals collector pipeline finished. ---")