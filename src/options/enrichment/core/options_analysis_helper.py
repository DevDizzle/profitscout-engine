# enrichment/core/options_analysis_helper.py
from __future__ import annotations
import datetime as dt
import math
import random
import time
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pandas_ta as ta
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

# CONFIG
PROJECT = "profitscout-lx6bb"
DATASET = "profit_scout"
PRICE_TABLE_ID = f"{PROJECT}.{DATASET}.price_data"
TARGET_TABLE_ID = f"{PROJECT}.{DATASET}.options_analysis_input"
STAGING_TABLE_ID = f"{PROJECT}.{DATASET}._stg_options_analysis"
METADATA_TABLE_ID = f"{PROJECT}.{DATASET}.stock_metadata"

RSI_LEN, SMA50, SMA200 = 14, 50, 200
ATM_DTE_MIN, ATM_DTE_MAX, ATM_MNY_PCT = 7, 90, 0.05


def ensure_table_exists(bq: bigquery.Client) -> None:
    """Create the options_analysis_input table if it doesn't exist."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{TARGET_TABLE_ID}` (
        ticker STRING, date DATE, open FLOAT64, high FLOAT64, low FLOAT64,
        adj_close FLOAT64, volume INT64, iv_avg FLOAT64, hv_30 FLOAT64,
        iv_industry_avg FLOAT64, iv_signal STRING, latest_rsi FLOAT64,
        latest_macd FLOAT64, latest_sma50 FLOAT64, latest_sma200 FLOAT64,
        close_30d_delta_pct FLOAT64, rsi_30d_delta FLOAT64, macd_30d_delta FLOAT64,
        close_90d_delta_pct FLOAT64, rsi_90d_delta FLOAT64, macd_90d_delta FLOAT64
    ) PARTITION BY date CLUSTER BY ticker
    """
    bq.query(ddl).result()


def ensure_staging_exists(bq: bigquery.Client) -> None:
    """Create the permanent staging table if it doesn't exist."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{STAGING_TABLE_ID}` (
        ticker STRING, date DATE, open FLOAT64, high FLOAT64, low FLOAT64,
        adj_close FLOAT64, volume INT64, iv_avg FLOAT64, hv_30 FLOAT64,
        iv_signal STRING, latest_rsi FLOAT64, latest_macd FLOAT64, latest_sma50 FLOAT64,
        latest_sma200 FLOAT64, close_30d_delta_pct FLOAT64, rsi_30d_delta FLOAT64,
        macd_30d_delta FLOAT64, close_90d_delta_pct FLOAT64, rsi_90d_delta FLOAT64,
        macd_90d_delta FLOAT64
    )
    """
    bq.query(ddl).result()


def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        xf = float(x)
        if math.isnan(xf) or math.isinf(xf):
            return None
        return xf
    except Exception:
        return None


def _normalize_row(row: Dict) -> Dict:
    out = dict(row)
    if not out.get("ticker"):
        raise ValueError("Row missing 'ticker'")
    d = out.get("date")
    if isinstance(d, (dt.date, dt.datetime)):
        out["date"] = d.strftime("%Y-%m-%d")
    elif isinstance(d, str):
        out["date"] = d[:10]
    else:
        raise ValueError("Row missing or invalid 'date'")
    return out


def _fetch_ohlcv_for_keys(
    bq: bigquery.Client, keys: List[Dict[str, str]]
) -> Dict[tuple, Dict]:
    if not keys:
        return {}
    tickers = list({k["ticker"] for k in keys})
    min_date = min(k["date"] for k in keys)
    max_date = max(k["date"] for k in keys)
    q = f"""
        SELECT ticker, CAST(date AS STRING) AS date_str, open, high, low, adj_close, volume
        FROM `{PRICE_TABLE_ID}`
        WHERE ticker IN UNNEST(@tickers) AND date BETWEEN @min_date AND @max_date
    """
    params = [
        bigquery.ArrayQueryParameter("tickers", "STRING", tickers),
        bigquery.ScalarQueryParameter("min_date", "DATE", min_date),
        bigquery.ScalarQueryParameter("max_date", "DATE", max_date),
    ]
    rows = bq.query(
        q, job_config=bigquery.QueryJobConfig(query_parameters=params)
    ).result()
    return {(r["ticker"], r["date_str"]): r for r in rows}


def _spot_on_or_before(
    bq: bigquery.Client, ticker: str, as_of: dt.date
) -> Optional[float]:
    q = (
        f"SELECT adj_close FROM `{PRICE_TABLE_ID}` "
        f"WHERE ticker = @ticker AND date <= @as_of ORDER BY date DESC LIMIT 1"
    )
    params = [
        bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
        bigquery.ScalarQueryParameter("as_of", "DATE", str(as_of)),
    ]
    rows = bq.query(
        q, job_config=bigquery.QueryJobConfig(query_parameters=params)
    ).result()
    for r in rows:
        return _safe_float(r["adj_close"])
    return None


def _merge_from_staging(bq: bigquery.Client, present_cols: List[str]) -> None:
    non_keys = [c for c in present_cols if c not in ("ticker", "date")]
    if not non_keys:
        return

    set_clause = ", ".join([f"T.{c} = COALESCE(S.{c}, T.{c})" for c in non_keys])
    insert_cols = ", ".join(present_cols)
    insert_vals = ", ".join([f"S.{c}" for c in present_cols])

    sql = f"""
    MERGE `{TARGET_TABLE_ID}` T USING `{STAGING_TABLE_ID}` S
    ON T.ticker = S.ticker AND T.date = S.date
    WHEN MATCHED THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    for attempt in range(6):
        try:
            bq.query(sql).result()
            return
        except BadRequest as e:
            if "Could not serialize access" in getattr(e, "message", str(e)):
                time.sleep((2**attempt) + random.random())
                continue
            raise


def upsert_analysis_rows(
    bq: bigquery.Client, rows: List[Dict], enrich_ohlcv: bool = True
) -> None:
    """Batch upsert using a permanent staging table (overwritten each run)."""
    if not rows:
        return

    ensure_table_exists(bq)
    ensure_staging_exists(bq)

    norm = [_normalize_row(r) for r in rows]

    if enrich_ohlcv:
        keys = [{"ticker": r["ticker"], "date": r["date"]} for r in norm]
        ohlcv = _fetch_ohlcv_for_keys(bq, keys)
        for r in norm:
            k = (r["ticker"], r["date"])
            if k in ohlcv:
                r.update(ohlcv[k])

    present_cols = sorted(
        {k for r in norm for k, v in r.items() if v is not None} | {"ticker", "date"}
    )

    load_cfg = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    bq.load_table_from_json(norm, STAGING_TABLE_ID, job_config=load_cfg).result()

    _merge_from_staging(bq, present_cols)


def compute_iv_avg_atm(
    full_chain_df: pd.DataFrame, underlying_price: Optional[float], as_of: dt.date
) -> Optional[float]:
    if full_chain_df is None or full_chain_df.empty or not underlying_price:
        return None
    df = full_chain_df.copy()
    df["expiration_date"] = pd.to_datetime(
        df["expiration_date"], errors="coerce"
    ).dt.date
    df["implied_volatility"] = pd.to_numeric(df["implied_volatility"], errors="coerce")
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df.dropna(subset=["expiration_date", "implied_volatility", "strike"], inplace=True)
    if df.empty:
        return None
    dte = (pd.to_datetime(df["expiration_date"]) - pd.to_datetime(as_of)).dt.days
    mny = np.abs(df["strike"] - underlying_price) / underlying_price
    atm_df = df.loc[(dte >= ATM_DTE_MIN) & (dte <= ATM_DTE_MAX) & (mny <= ATM_MNY_PCT)]
    if atm_df.empty:
        return None
    return _safe_float(atm_df["implied_volatility"].mean())


def _fetch_history_for_technicals(
    bq: bigquery.Client, ticker: str, as_of: dt.date
) -> pd.DataFrame:
    q = f"""
        SELECT date, open, high, low, adj_close AS close, volume
        FROM `{PRICE_TABLE_ID}`
        WHERE ticker = @ticker AND date <= @as_of AND date >= DATE_SUB(@as_of, INTERVAL 400 DAY)
        ORDER BY date ASC
    """
    params = [
        bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
        bigquery.ScalarQueryParameter("as_of", "DATE", str(as_of)),
    ]
    df = bq.query(
        q, job_config=bigquery.QueryJobConfig(query_parameters=params)
    ).to_dataframe()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.ffill().bfill().dropna(inplace=True)
    return df


def compute_technicals_and_deltas(
    price_hist: pd.DataFrame,
) -> Dict[str, Optional[float]]:
    out_keys = [
        "latest_rsi",
        "latest_macd",
        "latest_sma50",
        "latest_sma200",
        "close_30d_delta_pct",
        "rsi_30d_delta",
        "macd_30d_delta",
        "close_90d_delta_pct",
        "rsi_90d_delta",
        "macd_90d_delta",
    ]
    if price_hist is None or price_hist.empty:
        return {k: None for k in out_keys}

    df = price_hist.copy()
    df[f"RSI_{RSI_LEN}"] = ta.rsi(close=df["close"], length=RSI_LEN)
    macd = ta.macd(close=df["close"], fast=12, slow=26, signal=9)
    df["MACD_12_26_9"] = (
        macd.iloc[:, 0] if isinstance(macd, pd.DataFrame) and not macd.empty else np.nan
    )
    df["SMA_50"] = ta.sma(close=df["close"], length=SMA50)
    df["SMA_200"] = ta.sma(close=df["close"], length=SMA200)
    valid = df.dropna(subset=["close", "RSI_14", "MACD_12_26_9", "SMA_50", "SMA_200"])
    if valid.empty:
        return {k: None for k in out_keys}

    latest = valid.iloc[-1]
    out = {
        "latest_rsi": _safe_float(latest["RSI_14"]),
        "latest_macd": _safe_float(latest["MACD_12_26_9"]),
        "latest_sma50": _safe_float(latest["SMA_50"]),
        "latest_sma200": _safe_float(latest["SMA_200"]),
    }
    try:
        if len(valid) >= 31:
            ago_30 = valid.iloc[-31]
            out["close_30d_delta_pct"] = _safe_float(
                (latest["close"] - ago_30["close"]) / ago_30["close"] * 100.0
            )
            out["rsi_30d_delta"] = _safe_float(latest["RSI_14"] - ago_30["RSI_14"])
            out["macd_30d_delta"] = _safe_float(
                latest["MACD_12_26_9"] - ago_30["MACD_12_26_9"]
            )
        if len(valid) >= 90:
            ago_90 = valid.iloc[-90]
            out["close_90d_delta_pct"] = _safe_float(
                (latest["close"] - ago_90["close"]) / ago_90["close"] * 100.0
            )
            out["rsi_90d_delta"] = _safe_float(latest["RSI_14"] - ago_90["RSI_14"])
            out["macd_90d_delta"] = _safe_float(
                latest["MACD_12_26_9"] - ago_90["MACD_12_26_9"]
            )
    except Exception:
        pass
    return out


def compute_hv30(
    bq: bigquery.Client,
    ticker: str,
    as_of: dt.date,
    price_history_df: pd.DataFrame = None,
) -> Optional[float]:
    df_to_use = price_history_df
    if df_to_use is None:
        q = f"""
            SELECT date, adj_close AS close
            FROM `{PRICE_TABLE_ID}`
            WHERE ticker = @ticker AND date > DATE_SUB(@as_of, INTERVAL 45 DAY) AND date <= @as_of
            ORDER BY date
        """
        params = [
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
            bigquery.ScalarQueryParameter("as_of", "DATE", str(as_of)),
        ]
        df_to_use = bq.query(
            q, job_config=bigquery.QueryJobConfig(query_parameters=params)
        ).to_dataframe()

    if df_to_use.empty or len(df_to_use) < 2:
        return None

    df_to_use["log_return"] = np.log(df_to_use["close"] / df_to_use["close"].shift(1))
    std_dev = df_to_use["log_return"].std()

    return _safe_float(std_dev * np.sqrt(252))


def backfill_iv_industry_avg_for_date(bq: bigquery.Client, run_date: dt.date) -> None:
    """
    Calculates the industry average IV for a given date and backfills it
    into the options_analysis_input table.
    """
    print(f"Starting backfill for IV industry average for date: {run_date}")

    # --- THIS IS THE FIX ---
    # The subquery 'S' now calculates the industry averages and provides a complete
    # list of tickers and their corresponding industry average IV.
    # The WHERE clause that caused the error has been removed.
    sql = f"""
    MERGE `{TARGET_TABLE_ID}` T
    USING (
        WITH IndustryAverages AS (
            -- Step 1: Calculate the average IV for each industry for the given run_date
            SELECT
                m.industry,
                AVG(a.iv_avg) AS calculated_industry_avg
            FROM `{TARGET_TABLE_ID}` a
            JOIN (
                SELECT ticker, industry, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY quarter_end_date DESC) as rn
                FROM `{METADATA_TABLE_ID}`
            ) m ON a.ticker = m.ticker AND m.rn = 1
            WHERE a.date = @run_date
              AND a.iv_avg IS NOT NULL
              AND m.industry IS NOT NULL
            GROUP BY m.industry
        )
        -- Step 2: Map the calculated average back to each ticker in the target table
        SELECT
            t.ticker,
            t.date,
            ia.calculated_industry_avg
        FROM `{TARGET_TABLE_ID}` t
        JOIN (
            SELECT ticker, industry, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY quarter_end_date DESC) as rn
            FROM `{METADATA_TABLE_ID}`
        ) m ON t.ticker = m.ticker AND m.rn = 1
        JOIN IndustryAverages ia ON m.industry = ia.industry
        WHERE t.date = @run_date
    ) S
    ON T.ticker = S.ticker AND T.date = S.date
    WHEN MATCHED THEN
        UPDATE SET T.iv_industry_avg = S.calculated_industry_avg
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
        ]
    )

    try:
        query_job = bq.query(sql, job_config=job_config)
        query_job.result()
        print(
            f"Successfully backfilled IV industry averages for {run_date}. "
            f"{query_job.num_dml_affected_rows} rows were updated."
        )
    except Exception as e:
        print(f"An error occurred during IV industry average backfill: {e}")
        raise
