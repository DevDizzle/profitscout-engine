# ingestion/core/orchestrators/technicals_collector.py
import datetime
import logging
import math
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import pandas_ta as ta
from google.cloud import bigquery, storage

from .. import config
from ..gcs import get_tickers, upload_json_to_gcs

# Note: Removed PRICE_TABLE_ID and all MERGE/write logic


# ----------------------------
# Utilities & helpers
# ----------------------------

def _safe_float(x):
    """Convert to builtin float; nan/inf -> None; non-numeric -> None."""
    try:
        if x is None:
            return None
        xf = float(x)
        if math.isnan(xf) or math.isinf(xf):
            return None
        return xf
    except Exception:
        return None


# ----------------------------
# Data access & indicator calc
# ----------------------------

def _get_price_history_for_chunk(tickers: list[str], bq_client: bigquery.Client) -> pd.DataFrame:
    """Fetches OHLCV data for a chunk of tickers."""
    logging.info(f"Querying BigQuery for price history of {len(tickers)} tickers...")
    query = f"""
        SELECT ticker, date, open, high, low, adj_close AS close, volume
        FROM `{config.TECHNICALS_PRICE_TABLE_ID}`
        WHERE ticker IN UNNEST(@tickers)
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL {config.ROLLING_52_WEEK_WINDOW + 100} DAY)
        ORDER BY ticker, date ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tickers", "STRING", tickers)]
    )
    df = bq_client.query(query, job_config=job_config).to_dataframe()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    # Ensure numeric
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    logging.info(f"Query complete. Found data for {df['ticker'].nunique()} unique tickers.")
    return df


def _calculate_technicals_for_ticker(ticker: str, price_df: pd.DataFrame) -> dict | None:
    """
    CPU-bound work for a single ticker. Returns a dict:
      {
        "ticker": <str>,
        "as_of": <YYYY-MM-DD>,
        "technicals_timeseries": <list[dict]>,   # last 90 rows enriched
        "update_row": {                          # row to persist later (NOT written here)
           "ticker": <str>, "date": <YYYY-MM-DD>,
           "latest_rsi": <float>, ... deltas ...
        }
      }
    Never raises; on error returns {"ticker": ticker, "error": "..."} to keep workers picklable.
    """
    try:
        if price_df is None or price_df.empty:
            return {"ticker": ticker, "error": "no price data"}

        core_columns = ["open", "high", "low", "close", "volume"]
        # Impute / clean core OHLCV
        price_df[core_columns] = price_df[core_columns].ffill().bfill()
        price_df.dropna(subset=core_columns, inplace=True)
        if price_df.empty:
            return {"ticker": ticker, "error": "no valid OHLCV rows"}

        # Compute indicators specified in config.INDICATORS
        for ind_name, ind in config.INDICATORS.items():
            kind = ind.get("kind")
            params = ind.get("params", {})
            if not hasattr(ta, kind):
                logging.warning(f"[{ticker}] pandas_ta has no function '{kind}'")
                continue

            func = getattr(ta, kind)

            # Build arg map only for supported parameters in this function
            arg_map = {}
            for name in ("close", "open", "high", "low", "volume"):
                if name in func.__code__.co_varnames:
                    arg_map[name] = price_df[name]

            try:
                result = func(**params, **arg_map)
            except Exception as e:
                logging.warning(f"[{ticker}] Indicator '{kind}' failed: {e}")
                continue

            if isinstance(result, pd.Series):
                price_df[ind_name] = result
            elif isinstance(result, pd.DataFrame):
                price_df = price_df.join(result)
            else:
                logging.warning(f"[{ticker}] Unexpected result type for {kind}: {type(result)}")

        # 52w high/low
        price_df["52w_high"] = price_df["high"].rolling(window=config.ROLLING_52_WEEK_WINDOW, min_periods=1).max()
        price_df["52w_low"] = price_df["low"].rolling(window=config.ROLLING_52_WEEK_WINDOW, min_periods=1).min()

        # Optional percent ATR if present
        atr_col = None
        for c in price_df.columns:
            if c.upper().startswith("ATR") or c.upper().startswith("ATRR_"):
                atr_col = c
                break
        if atr_col and "close" in price_df:
            price_df["percent_atr"] = (price_df[atr_col] / price_df["close"]) * 100

        # Require KPI columns
        needed_for_kpis = ["RSI_14", "MACD_12_26_9", "SMA_50", "SMA_200"]
        valid = price_df.dropna(subset=["open", "high", "low", "close", "volume"] + needed_for_kpis)
        if valid.empty:
            return {"ticker": ticker, "error": "no row with required KPIs"}

        latest = valid.iloc[-1]
        max_date = latest["date"].date()

        # KPIs
        kpis = {
            "latest_rsi": _safe_float(latest.get("RSI_14")),
            "latest_macd": _safe_float(latest.get("MACD_12_26_9")),
            "latest_sma50": _safe_float(latest.get("SMA_50")),
            "latest_sma200": _safe_float(latest.get("SMA_200")),
        }

        # Deltas
        deltas = {}
        if len(valid) >= 31:
            ago_30 = valid.iloc[-31]
            try:
                deltas["close_30d_delta_pct"] = _safe_float((latest["close"] - ago_30["close"]) / ago_30["close"] * 100)
                deltas["rsi_30d_delta"] = _safe_float(latest.get("RSI_14", 0) - ago_30.get("RSI_14", 0))
                deltas["macd_30d_delta"] = _safe_float(latest.get("MACD_12_26_9", 0) - ago_30.get("MACD_12_26_9", 0))
            except Exception:
                pass

        if len(valid) >= 91:
            ago_90 = valid.iloc[-91]
            try:
                deltas["close_90d_delta_pct"] = _safe_float((latest["close"] - ago_90["close"]) / ago_90["close"] * 100)
                deltas["rsi_90d_delta"] = _safe_float(latest.get("RSI_14", 0) - ago_90.get("RSI_14", 0))
                deltas["macd_90d_delta"] = _safe_float(latest.get("MACD_12_26_9", 0) - ago_90.get("MACD_12_26_9", 0))
            except Exception:
                pass

        # Build row to persist later (NOT written here)
        update_row = {"ticker": ticker, "date": max_date}
        update_row.update({**kpis, **deltas})

        # Enriched last-90 rows for UI/API
        recent_df = price_df.tail(90).copy()
        if not recent_df.empty:
            recent_df["date"] = recent_df["date"].dt.strftime("%Y-%m-%d")
        timeseries_records = recent_df.to_dict(orient="records")

        return {
            "ticker": ticker,
            "as_of": datetime.date.today().isoformat(),
            "technicals_timeseries": timeseries_records,
            "update_row": update_row,  # collected for later write by shared helper
        }

    except Exception as e:
        return {"ticker": ticker, "error": str(e)}


# ----------------------------
# Orchestrator
# ----------------------------

def run_pipeline(storage_client: storage.Client, bq_client: bigquery.Client):
    """
    Runs the full pipeline:
      - Fetch list of tickers
      - For each chunk:
          - Query price history
          - Compute technicals in parallel
          - Upload JSON to GCS
          - Collect KPI/delta rows (no DB writes here)
    """
    logging.info("--- Parallel Technicals Pipeline Started ---")
    tickers = get_tickers(storage_client)
    if not tickers:
        logging.error("No tickers found. Exiting technicals pipeline.")
        return

    max_workers = config.MAX_WORKERS_TIERING.get("technicals_collector") or 4
    chunk_size = config.BATCH_SIZE or 50
    logging.info(f"Processing {len(tickers)} tickers in chunks of {chunk_size} (max_workers={max_workers}).")

    total_uploaded = 0
    total_errors = 0
    total_rows_collected = 0  # rows prepared for future persistence (same table as IV)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for i in range(0, len(tickers), chunk_size):
            chunk = tickers[i : i + chunk_size]
            price_data_chunk = _get_price_history_for_chunk(chunk, bq_client=bq_client)
            if price_data_chunk.empty:
                logging.warning("No price data for this chunk. Skipping.")
                continue

            grouped_by_ticker = {t: df for t, df in price_data_chunk.groupby("ticker")}
            futures = {
                executor.submit(_calculate_technicals_for_ticker, t, df.copy()): t
                for t, df in grouped_by_ticker.items()
            }

            uploaded = 0
            errors = 0
            update_rows = []

            for future in as_completed(futures):
                t = futures[future]
                result = future.result()

                if not result:
                    errors += 1
                    logging.error(f"[{t}] Worker returned empty result")
                    continue

                if "error" in result:
                    errors += 1
                    logging.error(f"[{t}] Worker error: {result['error']}")
                    continue

                # Upload JSON to GCS (UI/API consumption)
                try:
                    blob_path = f"{config.TECHNICALS_OUTPUT_FOLDER}{t}_technicals.json"
                    upload_json_to_gcs(storage_client, {
                        "ticker": result["ticker"],
                        "as_of": result["as_of"],
                        "technicals_timeseries": result["technicals_timeseries"],
                    }, blob_path)
                    uploaded += 1
                except Exception as e:
                    errors += 1
                    logging.error(f"[{t}] Upload to GCS failed: {e}")

                # Collect row for future write via shared metrics helper (no DB writes here)
                if row := result.get("update_row"):
                    update_rows.append(row)

            # Accumulate stats and keep rows for later persistence (table TBD)
            total_uploaded += uploaded
            total_errors += errors
            total_rows_collected += len(update_rows)

            # Optionally: hand off here to a shared writer once table name is decided.
            # from analysis.market_metrics_writer import write_technicals_metrics
            # write_technicals_metrics(bq_client, update_rows, target_table="project.dataset.market_metrics")  # TODO: set name

            logging.info(f"Chunk {i//chunk_size + 1}: uploaded={uploaded}, errors={errors}, collected_rows={len(update_rows)}")

    logging.info(f"--- Technicals collector finished. Uploaded={total_uploaded}, errors={total_errors}, collected_rows={total_rows_collected} ---")
