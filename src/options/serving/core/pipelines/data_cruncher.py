# serving/core/pipelines/data_cruncher.py
import logging
import pandas as pd
import json
import numpy as np
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from typing import Dict, Any, Optional

from .. import config, gcs

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s",
)
OUTPUT_PREFIX = "prep/"
MAX_WORKERS = 16

# --- Technical Indicator Calculation ---


def _calculate_technicals(price_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculates all required technical indicators from a raw price history DataFrame.
    This function makes the pipeline self-sufficient, removing the need for pandas-ta.
    """
    if price_df.empty:
        return {}

    df = price_df.copy().sort_values("date")

    # --- SMA ---
    df["latest_sma50"] = df["adj_close"].rolling(window=50).mean()

    # --- RSI (Manual Calculation) ---
    delta = df["adj_close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["latest_rsi"] = 100 - (100 / (1 + rs))

    # --- 30-Day Deltas ---
    if len(df) >= 31:
        df["close_30d_ago"] = df["adj_close"].shift(30)
        df["rsi_30d_ago"] = df["latest_rsi"].shift(30)
        df["close_30d_delta_pct"] = (
            (df["adj_close"] - df["close_30d_ago"]) / df["close_30d_ago"] * 100
        )
        df["rsi_30d_delta"] = df["latest_rsi"] - df["rsi_30d_ago"]

    # --- Historical Volatility (30-Day) ---
    df["log_return"] = np.log(df["adj_close"] / df["adj_close"].shift(1))
    df["hv_30"] = df["log_return"].rolling(window=30).std() * np.sqrt(252)

    # Return the latest valid row of indicators
    latest_indicators = df.iloc[-1]
    return latest_indicators.to_dict()


# --- Helper Functions ---


def _get_work_list() -> pd.DataFrame:
    """
    Fetches the official work list from the tickerlist.txt file in GCS.
    """
    logging.info("Fetching work list from GCS tickerlist.txt...")
    tickers = gcs.get_tickers()
    if not tickers:
        logging.critical("Ticker list from GCS is empty. No work to do.")
        return pd.DataFrame()

    df = pd.DataFrame(tickers, columns=["ticker"])
    logging.info(f"Work list created for {len(df)} tickers from GCS.")
    return df


def _delete_old_prep_files(ticker: str):
    """Deletes all prep JSON files for a given ticker."""
    prefix = f"{OUTPUT_PREFIX}{ticker}_"
    blobs_to_delete = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix)
    for blob_name in blobs_to_delete:
        try:
            gcs.delete_blob(config.GCS_BUCKET_NAME, blob_name)
        except Exception as e:
            logging.error(f"[{ticker}] Failed to delete old prep file {blob_name}: {e}")


def _get_industry_performance_map(client: bigquery.Client) -> Dict[str, float]:
    """
    Calculates the average 30-day price change for every industry directly from price_data.
    """
    logging.info("Calculating industry average 30-day performance from price_data...")
    query = f"""
        WITH PriceDataRanked AS (
            SELECT
                ticker,
                date,
                adj_close,
                LAG(adj_close, 30) OVER (PARTITION BY ticker ORDER BY date) as close_30_days_ago
            FROM `{config.PRICE_DATA_TABLE_ID}`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY)
        ),
        LatestPriceChange AS (
            SELECT
                ticker,
                100 * (adj_close - close_30_days_ago) / NULLIF(close_30_days_ago, 0) as close_30d_delta_pct
            FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY date DESC) as rn
                FROM PriceDataRanked
                WHERE close_30_days_ago IS NOT NULL
            )
            WHERE rn = 1
        ),
        LatestData AS (
            SELECT
                a.ticker,
                a.close_30d_delta_pct,
                m.industry
            FROM LatestPriceChange a
            JOIN (
                SELECT ticker, industry, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY quarter_end_date DESC) as rn
                FROM `{config.BUNDLER_STOCK_METADATA_TABLE_ID}`
            ) m ON a.ticker = m.ticker AND m.rn = 1
            WHERE m.industry IS NOT NULL AND a.close_30d_delta_pct IS NOT NULL
        )
        SELECT
            industry,
            AVG(close_30d_delta_pct) as avg_industry_change
        FROM LatestData
        GROUP BY industry
    """
    try:
        df = client.query(query).to_dataframe()
        if df.empty:
            logging.warning("Could not calculate industry performance averages.")
            return {}

        perf_map = df.set_index("industry")["avg_industry_change"].to_dict()
        logging.info(
            f"Successfully calculated performance for {len(perf_map)} industries."
        )
        return perf_map
    except Exception as e:
        logging.error(f"Failed to get industry performance map: {e}", exc_info=True)
        return {}


def _fetch_and_calculate_kpis(
    ticker: str, industry_map: Dict[str, float]
) -> Optional[str]:
    """
    Fetches price and metadata, calculates all KPIs, and generates the prep file.
    """
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    run_date_str = date.today().strftime("%Y-%m-%d")
    final_json = {"ticker": ticker, "runDate": run_date_str, "kpis": {}}

    try:
        # --- NEW SIMPLIFIED QUERY ---
        # Fetches 400 days of price history and the latest industry.
        query = f"""
            WITH price_history AS (
                SELECT date, adj_close, volume
                FROM `{config.PRICE_DATA_TABLE_ID}`
                WHERE ticker = @ticker AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY)
                ORDER BY date ASC
            ),
            metadata AS (
                SELECT industry
                FROM `{config.BUNDLER_STOCK_METADATA_TABLE_ID}`
                WHERE ticker = @ticker
                ORDER BY quarter_end_date DESC
                LIMIT 1
            )
            SELECT
                p.date, p.adj_close, p.volume,
                m.industry
            FROM price_history p
            CROSS JOIN metadata m
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
        )
        price_df = client.query(query, job_config=job_config).to_dataframe()

        if price_df.empty:
            logging.warning(
                f"[{ticker}] No price history found. Skipping prep file generation."
            )
            _delete_old_prep_files(ticker)
            return None

        # --- Calculate all technicals in pandas ---
        indicators = _calculate_technicals(price_df)
        latest_row = price_df.iloc[-1].to_dict()
        latest_row.update(indicators)  # Combine latest price with latest indicators

        # --- KPI 1: Trend Strength ---
        price = latest_row.get("adj_close")
        sma50 = latest_row.get("latest_sma50")
        price_date = latest_row.get("date")

        if pd.notna(price) and pd.notna(sma50):
            signal = "bullish" if price > sma50 else "bearish"
            final_json["kpis"]["trendStrength"] = {
                "value": "Above 50D MA" if signal == "bullish" else "Below 50D MA",
                "price": round(price, 2),
                "price_date": (
                    str(price_date.date())
                    if hasattr(price_date, "date")
                    else str(price_date)
                ),
                "sma50": round(sma50, 2),
                "signal": signal,
                "tooltip": "Compares the previous day's closing price to the 50-day moving average to identify the current trend.",
            }

        # --- KPI 2: RSI Momentum ---
        latest_rsi = latest_row.get("latest_rsi")
        rsi_delta = latest_row.get("rsi_30d_delta")
        if pd.notna(latest_rsi) and pd.notna(rsi_delta):
            rsi_30_days_ago = latest_rsi - rsi_delta
            signal = (
                "strengthening"
                if rsi_delta > 1
                else "weakening" if rsi_delta < -1 else "stable"
            )

            final_json["kpis"]["rsiMomentum"] = {
                "currentRsi": round(latest_rsi, 2),
                "rsi30DaysAgo": round(rsi_30_days_ago, 2),
                "signal": signal,
                "tooltip": "Compares the current 14-day RSI to its value 30 days ago to gauge momentum.",
            }

        # --- KPI 3: Volume Surge ---
        volume = latest_row.get("volume")
        avg_volume = price_df["volume"].tail(30).mean()  # Calculate 30d avg volume
        if pd.notna(volume) and pd.notna(avg_volume) and avg_volume > 0:
            surge_pct = (volume / avg_volume - 1) * 100
            final_json["kpis"]["volumeSurge"] = {
                "value": round(surge_pct, 2),
                "signal": "high" if surge_pct > 50 else "normal",
                "volume": int(volume),
                "avgVolume30d": int(round(avg_volume, 0)),
                "tooltip": "The percentage difference between the most recent trading day's volume and its 30-day average volume.",
            }

        # --- KPI 4: Historical Volatility ---
        hv_30 = latest_row.get("hv_30")
        if pd.notna(hv_30):
            final_json["kpis"]["historicalVolatility"] = {
                "value": round(hv_30 * 100, 2),
                "signal": (
                    "high" if hv_30 > 0.5 else "low" if hv_30 < 0.2 else "moderate"
                ),
                "tooltip": "The stock's actual (realized) volatility over the last 30 days.",
            }

        # --- KPI 5: 30-Day Price Change ---
        change_pct = latest_row.get("close_30d_delta_pct")
        industry = latest_row.get("industry")
        industry_avg = industry_map.get(industry) if industry else None

        if pd.notna(change_pct):
            signal = "positive" if change_pct > 0 else "negative"
            comparison_signal = None
            if industry_avg is not None:
                if change_pct > industry_avg:
                    comparison_signal = "outperforming"
                else:
                    comparison_signal = "underperforming"

            final_json["kpis"]["thirtyDayChange"] = {
                "value": round(change_pct, 2),
                "signal": signal,
                "industryAverage": (
                    round(industry_avg, 2) if industry_avg is not None else None
                ),
                "comparisonSignal": comparison_signal,
                "tooltip": "The stock's price change over the last 30 days, compared to its industry average.",
            }

        _delete_old_prep_files(ticker)
        output_blob_name = f"{OUTPUT_PREFIX}{ticker}_{run_date_str}.json"
        gcs.write_text(
            config.GCS_BUCKET_NAME, output_blob_name, json.dumps(final_json, indent=2)
        )
        logging.info(
            f"[{ticker}] Successfully generated and uploaded prep JSON with enhanced KPIs."
        )
        return output_blob_name

    except Exception as e:
        logging.error(f"[{ticker}] Failed during KPI calculation: {e}", exc_info=True)
        return None


def run_pipeline():
    """Orchestrates the data crunching pipeline."""
    logging.info("--- Starting Data Cruncher (Prep Stage) Pipeline ---")

    work_list_df = _get_work_list()
    if work_list_df.empty:
        return

    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    industry_performance_map = _get_industry_performance_map(client)

    processed_count = 0
    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS, thread_name_prefix="CruncherWorker"
    ) as executor:
        future_to_ticker = {
            executor.submit(
                _fetch_and_calculate_kpis, row["ticker"], industry_performance_map
            ): row["ticker"]
            for _, row in work_list_df.iterrows()
        }
        for future in as_completed(future_to_ticker):
            try:
                if future.result():
                    processed_count += 1
            except Exception as exc:
                logging.error(
                    f"Worker generated an unhandled exception: {exc}", exc_info=True
                )

    logging.info(
        f"--- Data Cruncher Pipeline Finished. Processed {processed_count} of {len(work_list_df)} tickers. ---"
    )
