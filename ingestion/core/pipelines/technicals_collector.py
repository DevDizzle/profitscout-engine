# ingestion/core/orchestrators/technicals_collector.py
import logging
import datetime
import pandas as pd
import pandas_ta as ta
from concurrent.futures import ProcessPoolExecutor, as_completed
from google.cloud import storage, bigquery
from .. import config
from ..gcs import get_tickers, upload_json_to_gcs

PRICE_TABLE_ID = f"{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.price_data"  # For MERGE inserts

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

    # Extract latest KPIs (from max_date row)
    latest = price_df.iloc[-1]
    max_date = latest['date'].date()
    kpis = {
        'latest_rsi': latest.get('RSI_14', None),
        'latest_macd': latest.get('MACD_12_26_9', None),
        'latest_sma50': latest.get('SMA_50', None),
        'latest_sma200': latest.get('SMA_200', None),
        # Add more based on your ta.strategy: e.g., 'latest_adx': latest.get('ADX_14', None),
        # 'latest_atr': latest.get(atr_col_name, None),
    }

    # Compute deltas (30-day and 90-day; use approx row indices assuming daily data)
    if len(price_df) >= 30:
        ago_30 = price_df.iloc[-31]  # 30 days back (iloc[-1] is today, -31 is ~30 days ago)
        ago_90 = price_df.iloc[-91] if len(price_df) >= 90 else price_df.iloc[0]  # Fallback to earliest if <90

        deltas = {
            'close_30d_delta_pct': ((latest['close'] - ago_30['close']) / ago_30['close']) * 100 if ago_30['close'] else None,
            'rsi_30d_delta': latest.get('RSI_14', 0) - ago_30.get('RSI_14', 0),
            'macd_30d_delta': latest.get('MACD_12_26_9', 0) - ago_30.get('MACD_12_26_9', 0),
            # Add more: e.g., 'sma50_30d_delta_pct': ((latest['SMA_50'] - ago_30['SMA_50']) / ago_30['SMA_50']) * 100,
            
            'close_90d_delta_pct': ((latest['close'] - ago_90['close']) / ago_90['close']) * 100 if ago_90['close'] else None,
            'rsi_90d_delta': latest.get('RSI_14', 0) - ago_90.get('RSI_14', 0),
            'macd_90d_delta': latest.get('MACD_12_26_9', 0) - ago_90.get('MACD_12_26_9', 0),
            # Add more as needed
        }
    else:
        deltas = {k: None for k in ['close_30d_delta_pct', 'rsi_30d_delta', ...]}  # Null if insufficient data

    # MERGE KPIs and deltas into price_data for max_date
    bq_client = bigquery.Client(project=config.PROJECT_ID)
    merge_values = ', '.join([f"{v or 'NULL'} AS {k}" for k, v in {**kpis, **deltas}.items()])
    merge_sets = ', '.join([f"{k} = S.{k}" for k in {**kpis, **deltas}])
    insert_cols = ', '.join([k for k in {**kpis, **deltas}])
    insert_vals = ', '.join([f"S.{k}" for k in {**kpis, **deltas}])
    
    merge_q = f"""
        MERGE `{PRICE_TABLE_ID}` T
        USING (SELECT '{ticker}' AS ticker, DATE('{max_date}') AS date, {merge_values})
        S ON T.ticker = S.ticker AND T.date = S.date
        WHEN MATCHED THEN
            UPDATE SET {merge_sets}
        WHEN NOT MATCHED THEN
            INSERT (ticker, date, {insert_cols}) 
            VALUES (S.ticker, S.date, {insert_vals})
    """
    bq_client.query(merge_q).result()
    logging.info(f"[{ticker}] Inserted/updated KPIs and deltas in price_data for {max_date}.")

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