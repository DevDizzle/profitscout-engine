# ingestion/core/orchestrators/technicals_collector.py
import datetime
import logging
import math
import random
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import pandas_ta as ta
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery, storage

from .. import config
from ..gcs import get_tickers, upload_json_to_gcs

PRICE_TABLE_ID = f"{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.price_data"  # MERGE target


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


def _build_schema_from_rows(rows):
    """
    Build a BigQuery schema from a list of dict rows.
    Assumes 'ticker' STRING, 'date' DATE, others FLOAT64.
    """
    assert rows, "No rows to build schema"
    keys = list(rows[0].keys())
    schema = []
    for k in keys:
        if k == "ticker":
            schema.append(bigquery.SchemaField(k, "STRING"))
        elif k == "date":
            schema.append(bigquery.SchemaField(k, "DATE"))
        else:
            schema.append(bigquery.SchemaField(k, "FLOAT"))
    return schema


def _merge_updates_in_batch(bq_client: bigquery.Client, rows: list[dict]):
    """
    Load rows (ticker, date, KPI columns) into a temp staging table, then single MERGE into PRICE_TABLE_ID.
    Retries on 'Could not serialize access...' with exponential backoff.
    """
    if not rows:
        return

    # Ensure types are JSON-loadable (date as YYYY-MM-DD string, floats or None)
    for r in rows:
        # normalize date -> YYYY-MM-DD
        if isinstance(r.get("date"), (datetime.date, datetime.datetime)):
            r["date"] = r["date"].strftime("%Y-%m-%d")
        # normalize floats
        for k, v in list(r.items()):
            if k in ("ticker", "date"):
                continue
            r[k] = _safe_float(v)

    staging_id = f"{config.PROJECT_ID}.{config.BIGQUERY_DATASET}._tmp_price_updates_{uuid.uuid4().hex}"

    # Create staging
    schema = _build_schema_from_rows(rows)
    bq_client.create_table(bigquery.Table(staging_id, schema=schema))

    try:
        # Load JSON to staging
        load_job = bq_client.load_table_from_json(rows, staging_id)
        load_job.result()

        # Build dynamic clauses
        example = rows[0]
        nonkey_cols = [c for c in example.keys() if c not in ("ticker", "date")]
        if not nonkey_cols:
            # Nothing to merge
            bq_client.delete_table(staging_id, not_found_ok=True)
            return

        set_clause = ", ".join([f"{c} = COALESCE(S.{c}, T.{c})" for c in nonkey_cols])
        insert_cols = ", ".join(["ticker", "date"] + nonkey_cols)
        insert_vals = ", ".join([f"S.{c}" for c in ["ticker", "date"] + nonkey_cols])

        merge_sql = f"""
            MERGE `{PRICE_TABLE_ID}` T
            USING `{staging_id}` S
            ON T.ticker = S.ticker AND T.date = S.date
            WHEN MATCHED THEN
              UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        # Retry on serialization conflicts
        for attempt in range(6):
            try:
                bq_client.query(merge_sql).result()
                break
            except BadRequest as e:
                msg = getattr(e, "message", str(e))
                if "Could not serialize access" in msg:
                    sleep_s = (2 ** attempt) + random.random()
                    logging.warning(f"MERGE serialization conflict; retrying in {sleep_s:.2f}s (attempt {attempt+1})")
                    time.sleep(sleep_s)
                    continue
                raise
    finally:
        # Best-effort cleanup
        try:
            bq_client.delete_table(staging_id, not_found_ok=True)
        except Exception:
            pass


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
        "update_row": {                          # row to merge into price_data
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
            # Many ta funcs accept these names; only pass what the func supports.
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
                # Use the returned column names directly
                price_df = price_df.join(result)
            else:
                logging.warning(f"[{ticker}] Unexpected result type for {kind}: {type(result)}")

        # 52w high/low
        price_df["52w_high"] = price_df["high"].rolling(window=config.ROLLING_52_WEEK_WINDOW, min_periods=1).max()
        price_df["52w_low"] = price_df["low"].rolling(window=config.ROLLING_52_WEEK_WINDOW, min_periods=1).min()

        # Optional percent ATR if present
        atr_col = None
        # Try common ATR column names
        for c in price_df.columns:
            if c.upper().startswith("ATR") or c.upper().startswith("ATRR_"):
                atr_col = c
                break
        if atr_col and "close" in price_df:
            price_df["percent_atr"] = (price_df[atr_col] / price_df["close"]) * 100

        # Prepare a "valid" df requiring only KPIs we need (do not drop rows for unrelated cols)
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

        # Deltas (guard lengths so we don't compute bad values)
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

        # Build update_row for BigQuery (no writes here)
        update_row = {"ticker": ticker, "date": max_date}
        update_row.update({**kpis, **deltas})

        # Build timeseries JSON (last 90 rows of the *enriched* frame)
        recent_df = price_df.tail(90).copy()
        if not recent_df.empty:
            recent_df["date"] = recent_df["date"].dt.strftime("%Y-%m-%d")
        timeseries_records = recent_df.to_dict(orient="records")

        return {
            "ticker": ticker,
            "as_of": datetime.date.today().isoformat(),
            "technicals_timeseries": timeseries_records,
            "update_row": update_row,
        }

    except Exception as e:
        # Never let exceptions escape from worker
        return {"ticker": ticker, "error": str(e)}


# ----------------------------
# Orchestrator
# ----------------------------

def run_pipeline(storage_client: storage.Client, bq_client: bigquery.Client):
    """Runs the full pipeline, processing tickers in parallel chunks with batched MERGE writes."""
    logging.info("--- Parallel Technicals Pipeline Started ---")
    tickers = get_tickers(storage_client)
    if not tickers:
        logging.error("No tickers found. Exiting technicals pipeline.")
        return

    max_workers = config.MAX_WORKERS_TIERING.get("technicals_collector") or 4
    chunk_size = config.BATCH_SIZE or 50
    logging.info(f"Processing {len(tickers)} tickers in chunks of {chunk_size} (max_workers={max_workers}).")

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

            update_rows = []
            uploaded = 0
            errors = 0

            for future in as_completed(futures):
                t = futures[future]
                result = future.result()  # worker never raises; returns error dict on failure

                if not result:
                    errors += 1
                    logging.error(f"[{t}] Worker returned empty result")
                    continue

                if "error" in result:
                    errors += 1
                    logging.error(f"[{t}] Worker error: {result['error']}")
                    continue

                # Upload JSON to GCS
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

                # Queue row for batch MERGE
                if row := result.get("update_row"):
                    update_rows.append(row)

            # Single MERGE for this chunk (with retry/backoff, COALESCE to avoid null-overwrites)
            try:
                _merge_updates_in_batch(bq_client, update_rows)
                if update_rows:
                    logging.info(f"Merged {len(update_rows)} KPI rows into price_data for chunk starting {i}.")
            except Exception as e:
                logging.error(f"Batch MERGE failed for chunk starting at {i}: {e}", exc_info=True)

            logging.info(f"Chunk {i//chunk_size + 1}: uploaded={uploaded}, errors={errors}")

    logging.info("--- Technicals collector pipeline finished. ---")
