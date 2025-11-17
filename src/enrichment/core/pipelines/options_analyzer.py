# enrichment/core/pipelines/options_analyzer.py

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from google.cloud import bigquery

from .. import config

# --- Configuration ---
MIN_SCORE = 0.50  # Refers to options_score from options_candidate_selector
MAX_WORKERS = 16
OUTPUT_TABLE_ID = (
    f"{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.options_analysis_signals"
)

# --- Heuristics ---
IV_CHEAP_RATIO = 0.90
IV_EXPENSIVE_RATIO = 1.20


def _load_df_to_bq(df: pd.DataFrame, table_id: str, project_id: str):
    """
    Truncates and loads a pandas DataFrame into a BigQuery table.
    If df is empty, we TRUNCATE the table to avoid stale rows.
    """
    client = bigquery.Client(project=project_id)

    if df.empty:
        logging.warning("DataFrame is empty. Truncating %s and exiting.", table_id)
        try:
            client.query(f"TRUNCATE TABLE `{table_id}`").result()
            logging.info("Truncated %s as input DataFrame was empty.", table_id)
        except Exception as e:
            logging.error(
                "Failed to truncate %s with empty DataFrame: %s",
                table_id,
                e,
                exc_info=True,
            )
        return

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logging.info("Loaded %s rows into BigQuery table: %s", job.output_rows, table_id)
    except Exception as e:
        logging.error("Failed to load DataFrame to %s: %s", table_id, e, exc_info=True)
        raise


# =========================
# Feature helpers
# =========================
def _safe_mid(bid: float, ask: float, last: float) -> float | None:
    if pd.notna(bid) and pd.notna(ask) and bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return last if pd.notna(last) and last > 0 else None


def _spread_pct(bid: float, ask: float, mid: float | None) -> float | None:
    """
    Bid/ask spread in percent terms.
    Safer: guard against bad ticks (ask < bid) and nonpositive mid.
    """
    if mid and mid > 0 and pd.notna(bid) and pd.notna(ask) and ask >= bid:
        return (ask - bid) / mid * 100.0
    return None


def _dte(expiration_date: str | pd.Timestamp, fetch_date: str | pd.Timestamp) -> int | None:
    if pd.isna(expiration_date) or pd.isna(fetch_date):
        return None
    e = pd.to_datetime(expiration_date).date()
    f = pd.to_datetime(fetch_date).date()
    return (e - f).days


def _moneyness_pct(option_type: str, spot: float, strike: float) -> float | None:
    """
    Signed moneyness in percentage terms, positive for favorable direction:
      - Calls: (spot - strike) / spot
      - Puts : (strike - spot) / spot
    """
    if pd.isna(spot) or pd.isna(strike) or spot == 0:
        return None
    base = (spot - strike) / spot
    return base if str(option_type).lower() == "call" else -base


def _breakeven_distance_pct(
    option_type: str,
    spot: float,
    strike: float,
    mid_price: float | None,
) -> float | None:
    """
    Distance from spot to breakeven in percent terms.
      - Call breakeven = strike + premium
      - Put  breakeven = strike - premium
    """
    if any(pd.isna(x) for x in (spot, strike, mid_price)) or spot == 0:
        return None
    if str(option_type).lower() == "call":
        breakeven = strike + mid_price
        return (breakeven - spot) / spot * 100.0
    else:
        breakeven = strike - mid_price
        return (spot - breakeven) / spot * 100.0


def _expected_move_pct(implied_volatility: float, dte: int, haircut: float = 0.85) -> float | None:
    """
    Approximate expected move (percent) over DTE using annualized IV.
    Returns percent (e.g., 5.2 means 5.2%).
    """
    if pd.isna(implied_volatility) or implied_volatility <= 0 or pd.isna(dte) or dte <= 0:
        return None
    return implied_volatility * (dte / 365.0) ** 0.5 * haircut * 100.0


def _price_bucketed_spread_ok(mid: float | None, spread_pct: float | None) -> bool:
    """
    Accept wider % spreads for higher-priced options.
    Note: options_candidate_selector already enforces <= ~10% raw spread,
    so this acts as a *secondary* check / fine-grained penalty.
    """
    if mid is None or spread_pct is None:
        return False
    if mid < 0.75:
        return spread_pct <= 10  # stricter at low prices
    if mid < 1.50:
        return spread_pct <= 12
    return spread_pct <= 15


def _get_volatility_signal(contract_iv: float, hv_30: float) -> str:
    """
    Compares a contract's IV to the stock's 30-day HV and returns
    'Cheap' / 'Expensive' / 'Fairly Priced' / 'N/A'.
    """
    if pd.isna(contract_iv) or pd.isna(hv_30) or hv_30 <= 0.01:
        return "N/A"
    ratio = contract_iv / hv_30
    if ratio > IV_EXPENSIVE_RATIO:
        return "Expensive"
    elif ratio < IV_CHEAP_RATIO:
        return "Cheap"
    else:
        return "Fairly Priced"


# --- Percentile → 5-tier signal (matches recommendations_generator) ---
def _get_signal_from_percentile(percentile: float) -> str:
    """
    Determines the 5-tier outlook signal from the score's percentile rank.
    Matches the logic in recommendations_generator.
    """
    if pd.isna(percentile):
        return "Neutral / Mixed"
    if percentile >= 0.85:
        return "Strongly Bullish"
    elif percentile >= 0.65:
        return "Moderately Bullish"
    elif percentile >= 0.35:
        return "Neutral / Mixed"
    elif percentile >= 0.15:
        return "Moderately Bearish"
    else:
        return "Strongly Bearish"


def _fetch_candidates_all() -> pd.DataFrame:
    """
    Fetches all candidates and joins them with:
      - latest options_analysis_input (for hv_30, RSI, etc.)
      - latest analysis_scores (for score_percentile → outlook_signal)
    Universe is already gated by options_candidate_selector:
      - DTE 7–90
      - 5–10% OTM (direction-aware)
      - OI >= 500, spread <= 10%
      - options_score >= MIN_SCORE
    """
    client = bigquery.Client(project=config.PROJECT_ID)
    project = config.PROJECT_ID
    dataset = config.BIGQUERY_DATASET

    query = f"""
    WITH LatestRun AS (
        SELECT MAX(selection_run_ts) AS max_ts
        FROM `{project}.{dataset}.options_candidates`
    ),
    candidates AS (
        SELECT *
        FROM `{project}.{dataset}.options_candidates`
        WHERE selection_run_ts = (SELECT max_ts FROM LatestRun)
          AND options_score >= {MIN_SCORE}
    ),
    latest_analysis AS (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM `{project}.{dataset}.options_analysis_input`
        )
        WHERE rn = 1
    ),
    latest_scores AS (
        SELECT ticker, score_percentile
        FROM (
            SELECT
                ticker,
                score_percentile,
                ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY run_date DESC) AS rn
            FROM `{project}.{dataset}.analysis_scores`
            WHERE score_percentile IS NOT NULL
        )
        WHERE rn = 1
    )
    SELECT
        c.*,
        a.hv_30,
        a.latest_rsi,
        a.latest_macd,
        a.latest_sma50,
        a.close_30d_delta_pct,
        s.score_percentile
    FROM candidates c
    JOIN latest_analysis a ON c.ticker = a.ticker
    JOIN latest_scores s ON c.ticker = s.ticker
    ORDER BY c.ticker, c.options_score DESC
    """

    df = client.query(query).to_dataframe()

    if df.empty:
        return df

    # Stringify dates for consistency / safety
    for col in ("selection_run_ts", "expiration_date", "fetch_date"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Normalize percentile if accidentally stored 0–100
    if df["score_percentile"].max() > 1.0:
        logging.warning("score_percentile > 1.0 detected, normalizing 0–100 to 0–1.")
        df["score_percentile"] = df["score_percentile"] / 100.0

    df["outlook_signal"] = df["score_percentile"].apply(_get_signal_from_percentile)

    return df


def _process_contract(row: pd.Series) -> dict | None:
    """
    Purely deterministic contract-level scoring aligned with:
      - options_candidate_selector universe
      - short_term_long_premium strategy
    """
    ticker = row["ticker"]
    csym = row.get("contract_symbol")

    bid = row.get("bid", None)
    ask = row.get("ask", None)
    last = row.get("last_price", None)
    mid_px = _safe_mid(bid, ask, last)
    spread = _spread_pct(bid, ask, mid_px)
    dte = _dte(row.get("expiration_date"), row.get("fetch_date"))
    be_pct = _breakeven_distance_pct(
        row.get("option_type"),
        row.get("underlying_price"),
        row.get("strike"),
        mid_px,
    )
    contract_iv = row.get("implied_volatility")
    hv_30 = row.get("hv_30")
    exp_move = _expected_move_pct(contract_iv, dte)

    direction_bull = row.get("outlook_signal") in ("Strongly Bullish", "Moderately Bullish")
    direction_bear = row.get("outlook_signal") in ("Strongly Bearish", "Moderately Bearish")
    is_call = str(row.get("option_type")).lower() == "call"
    is_put = str(row.get("option_type")).lower() == "put"
    aligned = (direction_bull and is_call) or (direction_bear and is_put)

    vol_cmp_signal = _get_volatility_signal(contract_iv, hv_30)
    vol_ok = vol_cmp_signal in ("Cheap", "Fairly Priced")
    spread_ok = _price_bucketed_spread_ok(mid_px, spread)
    be_ok = (be_pct is not None and exp_move is not None and be_pct <= exp_move)

    red_flags = 0
    if not aligned:
        red_flags += 1
    if vol_cmp_signal == "Expensive":
        red_flags += 1
    if not spread_ok:
        red_flags += 1
    if not be_ok:
        red_flags += 1
    if row.get("theta") is not None and row.get("theta") < -0.05 and (dte is not None and dte <= 7):
        red_flags += 1

    # --- Map to Strong / Fair / Weak deterministically ---
    if red_flags >= 2:
        quality = "Weak"
        summary = (
            "Multiple red flags for a short-term long-premium trade "
            "(direction, volatility, liquidity, breakeven, or theta)."
        )
    elif red_flags == 0 and aligned and vol_ok and spread_ok and be_ok:
        quality = "Strong"
        summary = (
            "Direction aligns with the option type, IV is not rich, liquidity is acceptable, "
            "and breakeven sits within a plausible expected move."
        )
    else:
        quality = "Fair"
        summary = (
            "Setup is mixed: some factors support the trade, but at least one of direction, "
            "volatility, liquidity, breakeven, or theta adds risk for a short-term long-premium entry."
        )

    try:
        return {
            "ticker": ticker,
            "run_date": row.get("fetch_date"),
            "expiration_date": row.get("expiration_date"),
            "strike_price": row.get("strike"),
            "implied_volatility": row.get("implied_volatility"),
            "volatility_comparison_signal": vol_cmp_signal,
            "stock_price_trend_signal": row.get("outlook_signal"),
            "setup_quality_signal": quality,
            "summary": summary,
            "contract_symbol": csym,
            "option_type": str(row.get("option_type")).lower()
            if row.get("option_type") is not None
            else None,
        }
    except Exception as e:
        logging.error(
            "[%s] Contract %s deterministic scoring failed: %s", ticker, csym, e, exc_info=True
        )
        return None


def run_pipeline():
    """
    Runs the contract-level deterministic decisioning pipeline and loads results to BigQuery.
    """
    logging.info("--- Starting Options Analysis Signal Generation (deterministic) ---")
    df = _fetch_candidates_all()
    if df.empty:
        logging.warning("No candidate contracts found. Exiting.")
        _load_df_to_bq(df, OUTPUT_TABLE_ID, config.PROJECT_ID)  # Truncate table
        return

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(_process_contract, row): row.get("contract_symbol")
            for _, row in df.iterrows()
        }
        for fut in as_completed(futures):
            contract_sym = futures[fut]
            try:
                result = fut.result()
                if result:
                    results.append(result)
            except Exception as e:
                logging.error(
                    "Future for contract %s failed unexpectedly: %s",
                    contract_sym,
                    e,
                    exc_info=True,
                )

    output_df = pd.DataFrame(results)
    logging.info("Generated %d signals. Loading to BigQuery...", len(output_df))
    _load_df_to_bq(output_df, OUTPUT_TABLE_ID, config.PROJECT_ID)
    logging.info(
        "--- Finished. Wrote %d signals to %s. ---",
        len(output_df),
        OUTPUT_TABLE_ID,
    )
