# serving/core/pipelines/price_chart_generator.py
import logging
import pandas as pd
import json
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery

from .. import config, gcs

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s")
# The output folder in GCS for the new JSON files
PRICE_CHART_JSON_FOLDER = "price-chart-json/"
MAX_WORKERS = 8

# --- Data Fetching and Processing ---

def _get_all_price_histories(tickers: list[str]) -> dict[str, pd.DataFrame]:
    """
    Fetches price history for all tickers in a single BigQuery call.
    Now includes OHLC and volume data needed for candlestick charts.
    """
    if not tickers:
        return {}
    
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    # --- THIS IS THE FIX ---
    # Increased lookback period to 450 days to ensure the 200-day SMA
    # has a sufficient "warm-up" period for the 90-day chart view.
    start_date = date.today() - pd.Timedelta(days=450)
    
    query = f"""
        SELECT ticker, date, open, high, low, adj_close, volume
        FROM `{config.PRICE_DATA_TABLE_ID}`
        WHERE ticker IN UNNEST(@tickers) AND date >= @start_date
        ORDER BY ticker, date ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("tickers", "STRING", tickers),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date.isoformat()),
        ]
    )
    full_df = client.query(query, job_config=job_config).to_dataframe()
    return {t: grp.copy() for t, grp in full_df.groupby("ticker")}

def _delete_old_price_json(ticker: str):
    """Deletes all previous price chart JSON files for a given ticker."""
    prefix = f"{PRICE_CHART_JSON_FOLDER}{ticker}_"
    blobs_to_delete = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix)
    for blob_name in blobs_to_delete:
        try:
            gcs.delete_blob(config.GCS_BUCKET_NAME, blob_name)
        except Exception as e:
            logging.error(f"[{ticker}] Failed to delete old price chart JSON {blob_name}: {e}")

def _generate_price_chart_json(ticker: str, price_df: pd.DataFrame) -> str | None:
    """
    Generates a 90-day price chart JSON object and uploads it to GCS.
    """
    if price_df is None or price_df.empty:
        logging.warning(f"[{ticker}] No price data provided for JSON generation.")
        return None

    df = price_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "adj_close", "open", "high", "low", "volume"]).sort_values("date")

    # Calculate moving averages
    if len(df) >= 50:
        df["sma_50"] = df["adj_close"].rolling(window=50).mean().round(2)
    if len(df) >= 200:
        df["sma_200"] = df["adj_close"].rolling(window=200).mean().round(2)
    
    # Take the last 90 days for the final output
    plot_df = df.tail(90)
    if plot_df.empty:
        return None

    # Format data for JSON output
    chart_data = {
        "candlestick": [
            {"date": row.date.strftime('%Y-%m-%d'), "open": row.open, "high": row.high, "low": row.low, "close": row.adj_close}
            for row in plot_df.itertuples()
        ],
        "volume": [
            {"date": row.date.strftime('%Y-%m-%d'), "value": int(row.volume)}
            for row in plot_df.itertuples()
        ],
        "sma50": [
            {"date": row.date.strftime('%Y-%m-%d'), "value": row.sma_50}
            for row in plot_df.itertuples() if hasattr(row, 'sma_50') and pd.notna(row.sma_50)
        ],
        "sma200": [
            {"date": row.date.strftime('%Y-%m-%d'), "value": row.sma_200}
            for row in plot_df.itertuples() if hasattr(row, 'sma_200') and pd.notna(row.sma_200)
        ],
    }
    
    today_str = date.today().strftime('%Y-%m-%d')
    blob_name = f"{PRICE_CHART_JSON_FOLDER}{ticker}_{today_str}.json"
    
    try:
        _delete_old_price_json(ticker)
        gcs.write_text(config.GCS_BUCKET_NAME, blob_name, json.dumps(chart_data), "application/json")
        logging.info(f"[{ticker}] Successfully uploaded price chart JSON to gs://{config.GCS_BUCKET_NAME}/{blob_name}")
        return blob_name
    except Exception as e:
        logging.error(f"[{ticker}] Failed to upload price chart JSON: {e}", exc_info=True)
        return None

def process_ticker(ticker: str, price_histories: dict):
    """Worker: prepares data and triggers the JSON generation for a single ticker."""
    price_df = price_histories.get(ticker)
    if price_df is None or price_df.empty:
        logging.warning(f"[{ticker}] No price data available for chart JSON.")
        return None
    
    return _generate_price_chart_json(ticker, price_df.copy())

def run_pipeline():
    """Orchestrates the price chart JSON generation."""
    logging.info("--- Starting Price Chart JSON Generation Pipeline ---")
    tickers = gcs.get_tickers()
    if not tickers:
        logging.critical("No tickers loaded. Exiting.")
        return
    
    price_histories = _get_all_price_histories(tickers)
    
    processed_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {executor.submit(process_ticker, t, price_histories): t for t in tickers}
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                if future.result():
                    processed_count += 1
            except Exception as e:
                logging.exception(f"[{ticker}] Unhandled error in worker: {e}")
    
    logging.info(f"--- Price Chart JSON Generation Finished. Processed {processed_count} of {len(tickers)} tickers. ---")