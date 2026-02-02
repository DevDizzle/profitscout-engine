# ingestion/core/pipelines/technicals_collector.py
import datetime
import logging
import math
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import pandas_ta as ta
from google.cloud import bigquery, storage

from .. import config
from ..gcs import get_tickers, upload_json_to_gcs

# ----------------------------
# Utilities & helpers
# ----------------------------


def _safe_float(x):
    try:
        if x is None:
            return None
        xf = float(x)
        if math.isnan(xf) or math.isinf(xf):
            return None
        return xf
    except Exception:
        return None


def _to_iso_date(d):
    try:
        s = str(d)
        return s[:10]  # 'YYYY-MM-DD'
    except Exception:
        return None


def _finite_float(x):
    try:
        xf = float(x)
        return xf if math.isfinite(xf) else None
    except Exception:
        return None


def _int_or_none(x):
    try:
        xi = int(x)
        return xi if xi >= 0 else None
    except Exception:
        return None


def _ensure_core_kpis(price_df: pd.DataFrame) -> pd.DataFrame:
    if "close" not in price_df.columns:
        raise KeyError("Expected 'close' column (aliased from adj_close) not found")
    close = price_df["close"]
    if "RSI_14" not in price_df.columns:
        try:
            price_df["RSI_14"] = ta.rsi(close, length=14)
        except Exception as e:
            logging.warning(f"RSI calc failed: {e}")
            price_df["RSI_14"] = pd.NA
    if "SMA_50" not in price_df.columns:
        try:
            price_df["SMA_50"] = ta.sma(close, length=50)
        except Exception as e:
            logging.warning(f"SMA_50 calc failed: {e}")
            price_df["SMA_50"] = pd.NA
    if "SMA_200" not in price_df.columns:
        try:
            price_df["SMA_200"] = ta.sma(close, length=200)
        except Exception as e:
            logging.warning(f"SMA_200 calc failed: {e}")
            price_df["SMA_200"] = pd.NA
    if "MACD_12_26_9" not in price_df.columns:
        try:
            macd_df = ta.macd(close, fast=12, slow=26, signal=9)
            if isinstance(macd_df, pd.DataFrame):
                if "MACD_12_26_9" in macd_df.columns:
                    price_df["MACD_12_26_9"] = macd_df["MACD_12_26_9"]
                else:
                    for c in macd_df.columns:
                        cu = c.upper()
                        if (
                            cu.startswith("MACD_")
                            and not cu.startswith("MACDS_")
                            and not cu.startswith("MACDH_")
                        ):
                            price_df["MACD_12_26_9"] = macd_df[c]
                            break
                    if "MACD_12_26_9" not in price_df.columns:
                        price_df["MACD_12_26_9"] = pd.NA
            else:
                price_df["MACD_12_26_9"] = macd_df
        except Exception as e:
            logging.warning(f"MACD calc failed: {e}")
            price_df["MACD_12_26_9"] = pd.NA
    return price_df


# --- NEW: build a clean indicators-only 90d payload (no OHLCV, no ticker) ---
def _build_technicals_payload(df: pd.DataFrame) -> list[dict]:
    """
    Return last-90 rows with only date + indicator fields (no OHLCV).
    NaN/Inf -> None; drop rows without date.
    """
    if df.empty:
        return []
    # Preferred indicator columns (add/remove as needed)
    prefer_cols = [
        "SMA_50",
        "SMA_200",
        "EMA_21",
        "MACD_12_26_9",
        "MACDs_12_26_9",
        "MACDh_12_26_9",
        "RSI_14",
        "ADX_14",
        "ADXR_14_2",
        "DMP_14",
        "DMN_14",
        "STOCHk_14_3_3",
        "STOCHd_14_3_3",
        "ROC_20",
        "BBL_20_2.0_2.0",
        "BBM_20_2.0_2.0",
        "BBU_20_2.0_2.0",
        "BBB_20_2.0_2.0",
        "BBP_20_2.0_2.0",
        "ATR",
        "OBV",
        "52w_high",
        "52w_low",
        "percent_atr",
    ]
    # Normalize aliases if present
    alias_map = {
        "ema_21": "EMA_21",
        "atr": "ATR",
        "obv": "OBV",
        "rsi_14": "RSI_14",
        "sma_50": "SMA_50",
        "sma_200": "SMA_200",
        "roc_20": "ROC_20",
    }
    df = df.copy()
    for src, dst in alias_map.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]

    keep_cols = ["date"] + [c for c in prefer_cols if c in df.columns]
    snap = df.tail(90)[keep_cols].copy()

    out = []
    for _, r in snap.iterrows():
        date = _to_iso_date(r.get("date"))
        if not date:
            continue
        row = {"date": date}
        for c in keep_cols[1:]:
            v = r.get(c)
            if isinstance(v, (int,)) and not isinstance(v, bool):
                row[c] = int(v)
            elif isinstance(v, float):
                row[c] = v if math.isfinite(v) else None
            else:
                # Pandas NA or other types
                try:
                    fv = float(v)
                    row[c] = fv if math.isfinite(fv) else None
                except Exception:
                    row[c] = None if pd.isna(v) else v
        out.append(row)
    return out


# ----------------------------
# Data access & indicator calc
# ----------------------------


def _get_price_history_for_chunk(
    tickers: list[str], bq_client: bigquery.Client
) -> pd.DataFrame:
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
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    logging.info(
        f"Query complete. Found data for {df['ticker'].nunique()} unique tickers."
    )
    return df


def _calculate_technicals_for_ticker(
    ticker: str, price_df: pd.DataFrame
) -> dict | None:
    try:
        if price_df is None or price_df.empty:
            return {"ticker": ticker, "error": "no price data"}

        core_columns = ["open", "high", "low", "close", "volume"]
        price_df[core_columns] = price_df[core_columns].ffill().bfill()
        price_df.dropna(subset=core_columns, inplace=True)
        if price_df.empty:
            return {"ticker": ticker, "error": "no valid OHLCV rows"}

        # Compute configured indicators
        for ind_name, ind in config.INDICATORS.items():
            kind = ind.get("kind")
            params = ind.get("params", {})
            if not hasattr(ta, kind):
                logging.warning(f"[{ticker}] pandas_ta has no function '{kind}'")
                continue
            func = getattr(ta, kind)
            arg_map = {}
            for name in ("close", "open", "high", "low", "volume"):
                if (
                    name
                    in getattr(
                        func, "__code__", type("x", (), {"co_varnames": ()})
                    ).co_varnames
                ):
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
                logging.warning(
                    f"[{ticker}] Unexpected result type for {kind}: {type(result)}"
                )

        # Ensure core KPIs
        price_df = _ensure_core_kpis(price_df)

        # 52w high/low
        price_df["52w_high"] = (
            price_df["high"]
            .rolling(window=config.ROLLING_52_WEEK_WINDOW, min_periods=1)
            .max()
        )
        price_df["52w_low"] = (
            price_df["low"]
            .rolling(window=config.ROLLING_52_WEEK_WINDOW, min_periods=1)
            .min()
        )

        # percent ATR if present
        atr_col = None
        for c in price_df.columns:
            cu = c.upper()
            if cu.startswith("ATR") or cu.startswith("ATRR_"):
                atr_col = c
                break
        if atr_col and "close" in price_df:
            price_df["percent_atr"] = (price_df[atr_col] / price_df["close"]) * 100

        # Bollinger Band Width (BBB) if present
        bbb_col = None
        for c in price_df.columns:
            cu = c.upper()
            if cu.startswith("BBB_"):
                bbb_col = c
                break

        # Valid KPI rows (optional for deltas; not used for the per-row payload)
        needed_for_kpis = ["RSI_14", "MACD_12_26_9", "SMA_50", "SMA_200"]
        valid = price_df.dropna(
            subset=["open", "high", "low", "close", "volume"] + needed_for_kpis
        )

        use_df = (
            valid
            if not valid.empty
            else price_df.dropna(subset=["open", "high", "low", "close", "volume"])
        )
        if use_df.empty:
            return {"ticker": ticker, "error": "no row with required OHLCV"}

        latest = use_df.iloc[-1]
        max_date = latest["date"].date()

        # KPIs (may be None if fallback path)
        kpis = {
            "latest_rsi": _safe_float(latest.get("RSI_14")),
            "latest_macd": _safe_float(latest.get("MACD_12_26_9")),
            "latest_sma50": _safe_float(latest.get("SMA_50")),
            "latest_sma200": _safe_float(latest.get("SMA_200")),
            "latest_atr": _safe_float(latest.get(atr_col)) if atr_col else None,
            "latest_bb_width": _safe_float(latest.get(bbb_col)) if bbb_col else None,
        }

        # Deltas (optional; retained for future persistence)
        deltas = {}
        if not valid.empty:
            if len(valid) >= 31:
                ago_30 = valid.iloc[-31]
                try:
                    deltas["close_30d_delta_pct"] = _safe_float(
                        (valid.iloc[-1]["close"] - ago_30["close"])
                        / ago_30["close"]
                        * 100
                    )
                    deltas["rsi_30d_delta"] = _safe_float(
                        valid.iloc[-1].get("RSI_14", 0) - ago_30.get("RSI_14", 0)
                    )
                    deltas["macd_30d_delta"] = _safe_float(
                        valid.iloc[-1].get("MACD_12_26_9", 0)
                        - ago_30.get("MACD_12_26_9", 0)
                    )
                except Exception:
                    pass
            if len(valid) >= 91:
                ago_90 = valid.iloc[-91]
                try:
                    deltas["close_90d_delta_pct"] = _safe_float(
                        (valid.iloc[-1]["close"] - ago_90["close"])
                        / ago_90["close"]
                        * 100
                    )
                    deltas["rsi_90d_delta"] = _safe_float(
                        valid.iloc[-1].get("RSI_14", 0) - ago_90.get("RSI_14", 0)
                    )
                    deltas["macd_90d_delta"] = _safe_float(
                        valid.iloc[-1].get("MACD_12_26_9", 0)
                        - ago_90.get("MACD_12_26_9", 0)
                    )
                except Exception:
                    pass

        update_row = {"ticker": ticker, "date": max_date}
        update_row.update({**kpis, **deltas})

        # --- NEW: indicators-only 90d payload for LLM (no OHLCV here) ---
        technicals_payload = _build_technicals_payload(price_df)

        # --- Keep building a 90d prices payload elsewhere in your pipeline (unchanged) ---
        # (If you already added a prices_payload helper, keep it; omitted here for brevity)

        return {
            "ticker": ticker,
            "as_of_date": datetime.date.today().isoformat(),
            "technicals": technicals_payload,
            "update_row": update_row,
        }

    except Exception as e:
        return {"ticker": ticker, "error": str(e)}


def run_pipeline(storage_client: storage.Client, bq_client: bigquery.Client):
    logging.info("--- Parallel Technicals Pipeline Started ---")
    tickers = get_tickers(storage_client)
    if not tickers:
        logging.error("No tickers found. Exiting technicals pipeline.")
        return

    max_workers = config.MAX_WORKERS_TIERING.get("technicals_collector") or 4
    chunk_size = config.BATCH_SIZE or 50
    logging.info(
        f"Processing {len(tickers)} tickers in chunks of {chunk_size} (max_workers={max_workers})."
    )

    total_uploaded = 0
    total_errors = 0
    all_update_rows = []  # Accumulator

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for i in range(0, len(tickers), chunk_size):
            chunk = tickers[i : i + chunk_size]
            price_data_chunk = _get_price_history_for_chunk(chunk, bq_client=bq_client)
            if price_data_chunk.empty:
                logging.warning("No price data for this chunk. Skipping.")
                continue

            grouped_by_ticker = dict(price_data_chunk.groupby("ticker"))
            futures = {
                executor.submit(_calculate_technicals_for_ticker, t, df.copy()): t
                for t, df in grouped_by_ticker.items()
            }

            uploaded = 0
            errors = 0

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

                # --- Upload technicals in the same top-level shape as prices file ---
                try:
                    tech_blob_path = (
                        f"{config.TECHNICALS_OUTPUT_FOLDER}{t}_technicals.json"
                    )
                    upload_json_to_gcs(
                        storage_client,
                        {
                            "ticker": result["ticker"],
                            "as_of_date": result["as_of_date"],
                            "technicals": result["technicals"],
                        },
                        tech_blob_path,
                    )
                    uploaded += 1
                except Exception as e:
                    errors += 1
                    logging.error(f"[{t}] Upload technicals failed: {e}")

                # (Optional) collect metrics row for future DB write
                if row := result.get("update_row"):
                    all_update_rows.append(row)

            total_uploaded += uploaded
            total_errors += errors
            logging.info(
                f"Chunk {i // chunk_size + 1}: uploaded={uploaded}, errors={errors}, collected_rows={len(all_update_rows)}"
            )

    # --- Persist to BigQuery History ---
    if all_update_rows:
        logging.info(
            f"Persisting {len(all_update_rows)} technical rows to BigQuery history..."
        )
        try:
            # --- Idempotency: Clean up any existing history for today ---
            # Technicals are calculated based on today's price, so we only want one entry per ticker per day.
            cleanup_query = f"DELETE FROM `{config.TECHNICALS_HISTORY_TABLE_ID}` WHERE date = CURRENT_DATE()"
            try:
                bq_client.query(cleanup_query).result()
                logging.info(
                    f"Cleaned up existing technicals history for today in {config.TECHNICALS_HISTORY_TABLE_ID}."
                )
            except Exception as e:
                logging.warning(
                    f"Cleanup query failed (possibly harmless if table is new): {e}"
                )

            df_hist = pd.DataFrame(all_update_rows)
            # Ensure date is date object or string? BQ pandas helper handles datetime.date usually.
            # Convert to appropriate types if needed.

            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ],
            )
            job = bq_client.load_table_from_dataframe(
                df_hist, config.TECHNICALS_HISTORY_TABLE_ID, job_config=job_config
            )
            job.result()
            logging.info("Successfully loaded technicals history.")
        except Exception as e:
            logging.error(f"Failed to persist technicals history: {e}", exc_info=True)

    logging.info(
        f"--- Technicals collector finished. Uploaded={total_uploaded}, errors={total_errors}, collected_rows={len(all_update_rows)} ---"
    )
