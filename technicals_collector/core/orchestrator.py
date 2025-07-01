# technicals_collector/core/orchestrator.py
import logging
import datetime
import pandas as pd
import pandas_ta as ta
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage, bigquery
from config import (
    MAX_WORKERS,
    GCS_BUCKET_NAME,
    GCS_OUTPUT_FOLDER,
    INDICATORS,
    PROJECT_ID,
    BIGQUERY_TABLE_ID,
    ROLLING_52_WEEK_WINDOW,
)
from core.gcs import get_tickers, upload_json_to_gcs

# Initialize BigQuery client once
try:
    bq_client = bigquery.Client(project=PROJECT_ID)
except Exception as e:
    logging.critical(f"Failed to initialize BigQuery client: {e}")
    bq_client = None

def get_all_price_histories(tickers: list[str]) -> pd.DataFrame:
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
    Cleans data, calculates indicators, and uploads the result for a single ticker.
    This function receives a DataFrame, so it does no network I/O.
    """
    try:
        logging.info(f"[{ticker}] Starting data processing.")
        
        # Data Cleaning Step
        core_columns = ['open', 'high', 'low', 'close', 'volume']
        price_df[core_columns] = price_df[core_columns].ffill().bfill()
        price_df.dropna(subset=core_columns, inplace=True)

        if price_df.empty:
            return f"[{ticker}] Skipped: No valid price data after cleaning."
        logging.info(f"[{ticker}] Data cleaning complete. DataFrame shape: {price_df.shape}")
        
        price_df = price_df.set_index("date")

        # --- Calculate Indicators with Robust Error Handling ---
        try:
            strategy = ta.Strategy(
                name="ProfitScout Standard",
                ta=[{"kind": ind["kind"], **ind["params"]} for ind in INDICATORS.values()],
            )
            price_df.ta.strategy(strategy, append=True)
            logging.info(f"[{ticker}] Technical indicators calculated.")
        except Exception as e:
            # If any error occurs during indicator calculation, skip this ticker
            logging.warning(f"[{ticker}] SKIPPING ticker due to indicator calculation error: {e}")
            return f"[{ticker}] Skipped due to indicator calculation error."

        # Post-processing for custom indicators
        price_df["52w_high"] = price_df["high"].rolling(window=ROLLING_52_WEEK_WINDOW, min_periods=1).max()
        price_df["52w_low"] = price_df["low"].rolling(window=ROLLING_52_WEEK_WINDOW, min_periods=1).min()
        atr_col_name = f"ATRr_{INDICATORS['atr']['params']['length']}"
        if atr_col_name in price_df.columns:
            price_df["percent_atr"] = (price_df[atr_col_name] / price_df["close"]) * 100
        logging.info(f"[{ticker}] Post-processing of indicators complete.")

        # Format data for JSON output
        price_df.index = price_df.index.strftime('%Y-%m-%d')
        records = price_df.to_dict(orient="index")
        technicals_data = {}

        def to_timeseries(keys_map):
            results = []
            for date, row in records.items():
                point = {"date": date}
                for out_key, in_key in keys_map.items():
                    point[out_key] = row.get(in_key)
                results.append(point)
            return results

        for name, ind in INDICATORS.items():
            params = ind['params']
            kind = ind['kind']
            if kind in ("sma", "ema", "rsi", "roc"):
                technicals_data[name] = to_timeseries({"value": f"{kind.upper()}_{params['length']}"})
            elif kind == "obv":
                technicals_data[name] = to_timeseries({"value": "OBV"})
            elif kind == "macd":
                p = params
                technicals_data[name] = to_timeseries({
                    "line": f"MACD_{p['fast']}_{p['slow']}_{p['signal']}",
                    "histogram": f"MACDh_{p['fast']}_{p['slow']}_{p['signal']}",
                    "signal": f"MACDs_{p['fast']}_{p['slow']}_{p['signal']}",
                })
            elif kind == "stoch":
                p = params
                technicals_data[name] = to_timeseries({
                    "k": f"STOCHk_{p['k']}_{p['d']}_{p['smooth_k']}",
                    "d": f"STOCHd_{p['k']}_{p['d']}_{p['smooth_k']}",
                })
            elif kind == "bbands":
                p = params
                technicals_data[name] = to_timeseries({
                    "lower": f"BBL_{p['length']}_{p['std']}",
                    "middle": f"BBM_{p['length']}_{p['std']}",
                    "upper": f"BBU_{p['length']}_{p['std']}",
                })
            elif kind == "atr":
                p = params
                technicals_data[name] = to_timeseries({
                    "value": f"ATRr_{p['length']}",
                    "percent_atr": "percent_atr",
                })
            elif kind == "adx":
                p = params
                technicals_data[name] = to_timeseries({
                    "adx": f"ADX_{p['length']}",
                    "dmp": f"DMP_{p['length']}",
                    "dmn": f"DMN_{p['length']}",
                })

        technicals_data["52w_high_low"] = to_timeseries({"high": "52w_high", "low": "52w_low"})
        logging.info(f"[{ticker}] JSON output structure created.")

        output_doc = { "ticker": ticker, "as_of": datetime.date.today().isoformat(), "technicals": technicals_data }
        blob_path = f"{GCS_OUTPUT_FOLDER}{ticker}_technicals.json"
        
        upload_json_to_gcs(storage_client, GCS_BUCKET_NAME, output_doc, blob_path)
        return f"[{ticker}] Technicals JSON uploaded successfully."

    except Exception as e:
        logging.error(f"[{ticker}] An unexpected error occurred during data processing: {e}", exc_info=True)
        return f"[{ticker}] Failed with error: {e}"


def run_pipeline(storage_client: storage.Client):
    """
    Runs the full technicals pipeline with enhanced logging and error handling.
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

    all_price_data = get_all_price_histories(tickers)
    if all_price_data.empty:
        logging.warning("Pipeline halting as no price data was returned from BigQuery.")
        return

    grouped_by_ticker = {ticker: df for ticker, df in all_price_data.groupby("ticker")}
    logging.info(f"Grouped data for {len(grouped_by_ticker)} tickers to prepare for processing.")

    logging.info(f"Starting ThreadPoolExecutor with a max of {MAX_WORKERS} workers.")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_ticker_data, ticker, price_df, storage_client): ticker
            for ticker, price_df in grouped_by_ticker.items()
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                logging.info(f"TASK_COMPLETE: {result}")
            except Exception as e:
                ticker = futures[future]
                logging.error(f"[{ticker}] A future completed with an unhandled exception: {e}", exc_info=True)

    logging.info("--- Technicals collection pipeline has finished all tasks. ---")