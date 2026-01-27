# src/enrichment/core/pipelines/options_analyzer.py

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from google.cloud import bigquery

from .. import config

# --- Configuration ---
MAX_WORKERS = 16
OUTPUT_TABLE_ID = (
    f"{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.options_analysis_signals"
)

# --- Heuristics ---
IV_CHEAP_RATIO = 0.85
IV_EXPENSIVE_RATIO = 1.50
NEGATIVE_GEX_THRESHOLD = -1000000 
POSITIVE_GEX_THRESHOLD = 1000000  


def _load_df_to_bq(df: pd.DataFrame, table_id: str, project_id: str):
    """
    Truncates and loads a pandas DataFrame into a BigQuery table.
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
    if any(pd.isna(x) for x in (spot, strike, mid_price)) or spot == 0:
        return None
    if str(option_type).lower() == "call":
        breakeven = strike + mid_price
        return (breakeven - spot) / spot * 100.0
    else:
        breakeven = strike - mid_price
        return (spot - breakeven) / spot * 100.0


def _expected_move_pct(implied_volatility: float, dte: int, haircut: float = 0.75) -> float | None:
    # UPDATED: Haircut reset to 0.75 (Was 1.0) for conservative projections.
    if pd.isna(implied_volatility) or implied_volatility <= 0 or pd.isna(dte) or dte <= 0:
        return None
    return implied_volatility * (dte / 365.0) ** 0.5 * haircut * 100.0


def _price_bucketed_spread_ok(mid: float | None, spread_pct: float | None) -> bool:
    if mid is None or spread_pct is None:
        return False
    # UPDATED: Relaxed buckets for Tier 1/2 trades (Quality of Life)
    if mid < 0.75:
        return spread_pct <= 25  # Was 10 (Too strict)
    if mid < 1.50:
        return spread_pct <= 20  # Was 12
    return spread_pct <= 15      # Standard


def _get_volatility_signal(contract_iv: float, hv_30: float) -> str:
    if pd.isna(contract_iv) or pd.isna(hv_30) or hv_30 <= 0.01:
        return "N/A"
    ratio = contract_iv / hv_30
    if ratio > IV_EXPENSIVE_RATIO:
        return "Expensive"
    elif ratio < IV_CHEAP_RATIO:
        return "Cheap"
    else:
        return "Fairly Priced"


def _get_signal_from_percentile(percentile: float) -> str:
    if pd.isna(percentile):
        return "Neutral / Mixed"
    if percentile >= 0.80: # Updated to 80th percentile
        return "Strongly Bullish"
    elif percentile >= 0.65:
        return "Moderately Bullish"
    elif percentile >= 0.35:
        return "Neutral / Mixed"
    elif percentile >= 0.20: # Updated to 20th percentile
        return "Moderately Bearish"
    else:
        return "Strongly Bearish"


def _fetch_candidates_all() -> pd.DataFrame:
    """
    Fetches candidates joined with ticker-level features (Total GEX) and Scores.
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
        SELECT ticker, score_percentile, news_score
        FROM (
            SELECT
                ticker,
                score_percentile,
                news_score, 
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
        a.total_gex,
        a.call_wall,
        a.put_wall,
        a.max_pain,
        a.put_call_vol_ratio AS market_pc_ratio,
        a.net_call_gamma,
        s.score_percentile,
        s.news_score
    FROM candidates c
    JOIN latest_analysis a ON c.ticker = a.ticker
    LEFT JOIN latest_scores s ON c.ticker = s.ticker 
    ORDER BY c.ticker, c.options_score DESC
    """

    df = client.query(query).to_dataframe()

    if df.empty:
        return df

    for col in ("selection_run_ts", "expiration_date", "fetch_date"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    if df["score_percentile"].max() > 1.0:
        logging.warning("score_percentile > 1.0 detected, normalizing 0–100 to 0–1.")
        df["score_percentile"] = df["score_percentile"] / 100.0

    df["outlook_signal"] = df["score_percentile"].apply(_get_signal_from_percentile)

    return df


def _process_contract(row: pd.Series) -> dict | None:
    """
    Contract-level scoring with 'Strategy' and 'Market Structure' Awareness.
    STRICT MODE: Only Trend Following (Alignment Required). No ML Sniper.
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
    
    total_gex = row.get("total_gex", 0)
    is_uoa = row.get("is_uoa", False)
    
    # Market Structure (Walls & Flow)
    call_wall = row.get("call_wall")
    put_wall = row.get("put_wall")
    pc_ratio = row.get("market_pc_ratio") or row.get("pc_ratio") # Prefer analysis, fallback to candidate
    strike = row.get("strike", 0)
    
    # [NEW] Strategy Tag - Standard or Conviction only
    strategy = "STANDARD"
    is_conviction = False
    
    # Fallback identification
    score_pct = row.get("score_percentile", 0.5)
    news_score = row.get("news_score", 0.0)
    if pd.isna(news_score): news_score = 0.0
    if pd.isna(score_pct): score_pct = 0.5
    
    # Promote to Conviction if strong sentiment aligns
    if (score_pct >= 0.80 or score_pct <= 0.20 or news_score >= 0.90):
        is_conviction = True
        strategy = "CONVICTION"

    direction_bull = row.get("outlook_signal") in ("Strongly Bullish", "Moderately Bullish")
    direction_bear = row.get("outlook_signal") in ("Strongly Bearish", "Moderately Bearish")
    is_call = str(row.get("option_type")).lower() == "call"
    is_put = str(row.get("option_type")).lower() == "put"
    
    # STRICT ALIGNMENT REQUIRED
    aligned = (direction_bull and is_call) or (direction_bear and is_put)

    vol_cmp_signal = _get_volatility_signal(contract_iv, hv_30)
    
    # --- Market Structure Checks ---
    structure_warning = []
    
    # 1. Wall Check (Don't buy calls ABOVE the Call Wall unless it's a breakout)
    if is_call and call_wall and strike > call_wall:
         structure_warning.append("Strike > Call Wall (Resistance)")
    
    # 2. Sentiment Check
    if is_call and pc_ratio and pc_ratio > 2.0:
        structure_warning.append("Bearish Flow (High P/C Ratio)")
    
    # --- Dynamic Risk Management (Forgiveness Logic) ---
    if is_conviction:
         vol_ok = True 
    else:
        vol_ok = vol_cmp_signal in ("Cheap", "Fairly Priced")

    if is_conviction:
         spread_ok = (spread is not None and spread <= 20)
    else:
        spread_ok = _price_bucketed_spread_ok(mid_px, spread)

    if is_conviction:
        be_ok = (be_pct is not None and exp_move is not None and be_pct <= (exp_move * 1.2))
    else:
        be_ok = (be_pct is not None and exp_move is not None and be_pct <= exp_move)

    red_flags = 0
    if not aligned: # Strict alignment failure
        red_flags += 1
    if not vol_ok:
        red_flags += 1
    if not spread_ok:
        red_flags += 1
    if not be_ok:
        red_flags += 1
    if len(structure_warning) > 0:
        red_flags += 1
    
    if not is_conviction:
        if row.get("theta") is not None and row.get("theta") < -0.05 and (dte is not None and dte <= 7):
            red_flags += 1

    # --- Scoring Logic ---
    quality = "Fair"
    summary_parts = []

    if is_conviction and aligned and red_flags == 0:
        quality = "Strong"
        summary_parts.append("CONVICTION PLAY: Strong fundamental tailwinds align with trend.")

    elif red_flags == 0 and aligned and vol_ok and spread_ok and be_ok:
        quality = "Strong"
        summary_parts.append("Standard Setup: Direction aligns with market structure.")
    elif red_flags >= 2:
        quality = "Weak"
        summary_parts.append("Multiple risks detected (Trend/Structure mismatch).")
    else:
        summary_parts.append("Mixed setup.")

    if structure_warning:
        summary_parts.append(f"Caution: {', '.join(structure_warning)}.")

    if is_uoa:
        summary_parts.append("Unusual Options Activity (Vol > OI).")
        if quality == "Weak": quality = "Fair" # Bump slightly but keep cautious

    if total_gex and total_gex < NEGATIVE_GEX_THRESHOLD:
        summary_parts.append("Negative Gamma Regime (Volatile).")
    elif total_gex and total_gex > POSITIVE_GEX_THRESHOLD:
        summary_parts.append("Positive Gamma Regime (Pinned).")

    summary = " ".join(summary_parts)

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
            "options_score": row.get("options_score"),
            "strategy": strategy 
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
    logging.info("--- Starting Options Analysis Signal Generation (UOA + GEX + ML Sniper) ---")
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