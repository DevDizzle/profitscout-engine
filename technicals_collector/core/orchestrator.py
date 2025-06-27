# technicals_collector/core/orchestrator.py
import logging
import datetime
import pandas as pd
import pandas_ta as ta
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache

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

@lru_cache(maxsize=MAX_WORKERS * 2)
def get_price_history(ticker: str) -> pd.DataFrame:
    """
    Fetches OHLCV data for a single ticker from BigQuery.
    Results are cached in memory per worker thread.
    """
    if not bq_client:
        raise ConnectionError("BigQuery client is not initialized.")

    logging.info(f"[{ticker}] Cache miss. Fetching price history from BigQuery.")
    # Fetch an extra 100 days to ensure enough data for long-period indicators like SMA-200
    query = f"""
        SELECT date, open, high, low, adj_close as close, volume
        FROM `{BIGQUERY_TABLE_ID}`
        WHERE ticker = @ticker
        AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL {ROLLING_52_WEEK_WINDOW + 100} DAY)
        ORDER BY date ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
    )
    df = bq_client.query(query, job_config=job_config).to_dataframe()

    if df.empty:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    return df

def calculate_indicators(df: pd.DataFrame) -> dict:
    """
    Calculates all configured technical indicators using pandas-ta.
    """
    if df.empty:
        return {}

    # Create a custom strategy from the indicator config
    strategy = ta.Strategy(
        name="ProfitScout Standard",
        ta=[{"kind": ind["kind"], **ind["params"]} for ind in INDICATORS.values()],
    )

    df.ta.strategy(strategy, append=True)

    # --- Post-processing for custom indicators ---
    # 52-week high/low breakouts
    df["52w_high"] = df["high"].rolling(window=ROLLING_52_WEEK_WINDOW, min_periods=1).max()
    df["52w_low"] = df["low"].rolling(window=ROLLING_52_WEEK_WINDOW, min_periods=1).min()

    # %ATR (ATR / Close)
    atr_col = f"ATRr_{INDICATORS['atr']['params']['length']}"
    if atr_col in df.columns:
        df["percent_atr"] = (df[atr_col] / df["close"]) * 100

    # Return as a dictionary of dates, which is easier to process
    return df.to_dict(orient="index")


def process_ticker(ticker: str, storage_client: storage.Client) -> str:
    """
    Orchestrates fetching data, calculating indicators, and uploading the JSON result.
    """
    try:
        ohlcv_df = get_price_history(ticker)
        if ohlcv_df.empty:
            return f"{ticker}: No price data in BigQuery, skipping."

        technicals_data = calculate_indicators(ohlcv_df)
        if not technicals_data:
            return f"{ticker}: Indicator calculation failed."

        # --- Conform to the existing JSON writer structure ---
        output = {
            "ticker": ticker,
            "as_of": datetime.date.today().isoformat(),
            "technicals": {},
        }
        
        dates = list(technicals_data.keys())

        # Pre-calculate complex keys to simplify list comprehensions
        sma_50_key = f"SMA_{INDICATORS['sma_50']['params']['length']}"
        sma_200_key = f"SMA_{INDICATORS['sma_200']['params']['length']}"
        ema_21_key = f"EMA_{INDICATORS['ema_21']['params']['length']}"
        rsi_14_key = f"RSI_{INDICATORS['rsi_14']['params']['length']}"
        roc_20_key = f"ROC_{INDICATORS['roc_20']['params']['length']}"
        
        m_params = INDICATORS['macd']['params']
        macd_line_key = f"MACD_{m_params['fast']}_{m_params['slow']}_{m_params['signal']}"
        macd_hist_key = f"MACDh_{m_params['fast']}_{m_params['slow']}_{m_params['signal']}"
        macd_signal_key = f"MACDs_{m_params['fast']}_{m_params['slow']}_{m_params['signal']}"

        s_params = INDICATORS['stochastic']['params']
        stoch_k_key = f"STOCHk_{s_params['k']}_{s_params['d']}_{s_params['smooth_k']}"
        stoch_d_key = f"STOCHd_{s_params['k']}_{s_params['d']}_{s_params['smooth_k']}"

        b_params = INDICATORS['bollinger_bands']['params']
        bbl_key = f"BBL_{b_params['length']}_{b_params['std']}"
        bbm_key = f"BBM_{b_params['length']}_{b_params['std']}"
        bbu_key = f"BBU_{b_params['length']}_{b_params['std']}"
        
        atr_params = INDICATORS['atr']['params']
        atr_key = f"ATRr_{atr_params['length']}"
        
        adx_params = INDICATORS['adx']['params']
        adx_key = f"ADX_{adx_params['length']}"
        dmp_key = f"DMP_{adx_params['length']}"
        dmn_key = f"DMN_{adx_params['length']}"

        # Populate the output dictionary
        output["technicals"]["sma_50"] = [{"date": d.strftime('%Y-%m-%d'), "value": row.get(sma_50_key)} for d, row in technicals_data.items()]
        output["technicals"]["sma_200"] = [{"date": d.strftime('%Y-%m-%d'), "value": row.get(sma_200_key)} for d, row in technicals_data.items()]
        output["technicals"]["ema_21"] = [{"date": d.strftime('%Y-%m-%d'), "value": row.get(ema_21_key)} for d, row in technicals_data.items()]
        output["technicals"]["rsi_14"] = [{"date": d.strftime('%Y-%m-%d'), "value": row.get(rsi_14_key)} for d, row in technicals_data.items()]
        output["technicals"]["roc_20"] = [{"date": d.strftime('%Y-%m-%d'), "value": row.get(roc_20_key)} for d, row in technicals_data.items()]
        output["technicals"]["obv"] = [{"date": d.strftime('%Y-%m-%d'), "value": row.get("OBV")} for d, row in technicals_data.items()]
        output["technicals"]["macd"] = [{"date": d.strftime('%Y-%m-%d'), "line": row.get(macd_line_key), "histogram": row.get(macd_hist_key), "signal": row.get(macd_signal_key)} for d, row in technicals_data.items()]
        output["technicals"]["stochastic"] = [{"date": d.strftime('%Y-%m-%d'), "k": row.get(stoch_k_key), "d": row.get(stoch_d_key)} for d, row in technicals_data.items()]
        output["technicals"]["bollinger_bands"] = [{"date": d.strftime('%Y-%m-%d'), "lower": row.get(bbl_key), "middle": row.get(bbm_key), "upper": row.get(bbu_key)} for d, row in technicals_data.items()]
        output["technicals"]["atr_14"] = [{"date": d.strftime('%Y-%m-%d'), "value": row.get(atr_key), "percent_atr": row.get("percent_atr")} for d, row in technicals_data.items()]
        output["technicals"]["adx_14"] = [{"date": d.strftime('%Y-%m-%d'), "adx": row.get(adx_key), "dmp": row.get(dmp_key), "dmn": row.get(dmn_key)} for d, row in technicals_data.items()]
        output["technicals"]["52w_high_low"] = [{"date": d.strftime('%Y-%m-%d'), "high": row.get("52w_high"), "low": row.get("52w_low")} for d, row in technicals_data.items()]

        if not any(output["technicals"].values()):
             return f"{ticker}: No technical data was generated."

        blob_path = f"{GCS_OUTPUT_FOLDER}{ticker}_technicals.json"
        upload_json_to_gcs(storage_client, GCS_BUCKET_NAME, output, blob_path)
        return f"{ticker}: Technicals JSON uploaded to {blob_path}."

    except Exception as e:
        logging.error(f"{ticker}: An unexpected error occurred: {e}", exc_info=True)
        return f"{ticker}: Failed with error: {e}"


def run_pipeline(storage_client: storage.Client):
    """
    Runs the full technicals collection pipeline using BigQuery and pandas-ta.
    """
    if not bq_client:
        logging.error("BigQuery client not initialized. Aborting pipeline.")
        return

    tickers = get_tickers(storage_client, GCS_BUCKET_NAME)
    if not tickers:
        logging.error("No tickers found. Exiting.")
        return

    logging.info(f"Starting technicals collection for {len(tickers)} tickers.")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ticker, t, storage_client): t for t in tickers}
        for future in as_completed(futures):
            try:
                result = future.result()
                logging.info(result)
            except Exception as e:
                ticker = futures[future]
                logging.error(f"{ticker}: An error occurred in the main loop: {e}", exc_info=True)

    logging.info("Technicals collection pipeline complete.")