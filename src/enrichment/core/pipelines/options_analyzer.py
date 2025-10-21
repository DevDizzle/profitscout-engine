# enrichment/core/pipelines/options_analyzer.py
import logging
import re
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from google.cloud import bigquery
from .. import config
from ..clients import vertex_ai

# --- Configuration ---
MIN_SCORE = 0.50
MAX_WORKERS = 16
OUTPUT_TABLE_ID = (
    f"{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.options_analysis_signals"
)

# --- Example output contract ---
_EXAMPLE_OUTPUT = """{
  "setup_quality": "Strong",
  "summary": "Cheap IV versus recent realized volatility, tight spread, and bullish trend make the risk/reward favorable.",
  "drivers": ["volatility","liquidity","alignment","breakeven","decay"],
  "confidence": 0.78
}"""

# =========================
# Feature helpers (NEW)
# =========================

def _safe_mid(bid: float, ask: float, last: float) -> float:
    if pd.notna(bid) and pd.notna(ask) and bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return last if pd.notna(last) and last > 0 else None

def _spread_pct(bid: float, ask: float, mid: float):
    if mid and mid > 0 and pd.notna(bid) and pd.notna(ask):
        return ((ask - bid) / mid) * 100.0
    return None

def _dte(expiration_date: str | pd.Timestamp, fetch_date: str | pd.Timestamp):
    if pd.isna(expiration_date) or pd.isna(fetch_date):
        return None
    # ensure date objects
    e = pd.to_datetime(expiration_date).date()
    f = pd.to_datetime(fetch_date).date()
    return (e - f).days

def _moneyness_pct(option_type: str, spot: float, strike: float):
    if pd.isna(spot) or pd.isna(strike) or spot == 0:
        return None
    base = (spot - strike) / spot
    return base if str(option_type).lower() == "call" else -base

def _breakeven_distance_pct(option_type: str, spot: float, strike: float, mid_price: float):
    if any(pd.isna(x) for x in (spot, strike, mid_price)) or spot == 0:
        return None
    if str(option_type).lower() == "call":
        breakeven = strike + mid_price
        return (breakeven - spot) / spot * 100.0
    else:
        breakeven = strike - mid_price
        return (spot - breakeven) / spot * 100.0

# --- NEW VOLATILITY SIGNAL FUNCTION (contract IV vs HV-30) ---
def _get_volatility_signal(contract_iv: float, hv_30: float) -> str:
    """
    Compares a contract's IV to the stock's 30-day HV and returns
    'Cheap' / 'Expensive' / 'Fairly Priced' / 'N/A'.
    """
    if pd.isna(contract_iv) or pd.isna(hv_30) or hv_30 <= 0.01:
        return "N/A"
    ratio = contract_iv / hv_30
    if ratio > 1.25:     # > +25% vs realized
        return "Expensive"
    elif ratio < 0.80:   # < -20% vs realized
        return "Cheap"
    else:
        return "Fairly Priced"

# --- Signal bucketing for weighted_score ---
def _get_signal_from_score(score: float) -> str:
    if score >= 0.75:
        return "Strongly Bullish"
    elif 0.60 <= score < 0.75:
        return "Moderately Bullish"
    elif 0.40 <= score < 0.60:
        return "Neutral / Mixed"
    elif 0.25 <= score < 0.40:
        return "Moderately Bearish"
    else:
        return "Strongly Bearish"

# =========================
# Data fetch (trimmed to needed cols & removed old iv_avg/iv_signal)
# =========================
def _fetch_candidates_all() -> pd.DataFrame:
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
        SELECT ticker, weighted_score
        FROM (
            SELECT ticker, weighted_score, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY run_date DESC) AS rn
            FROM `{project}.{dataset}.analysis_scores`
            WHERE weighted_score IS NOT NULL
        )
        WHERE rn = 1
    )
    SELECT
        c.*,
        a.hv_30,
        a.latest_rsi,
        a.latest_macd,
        a.latest_sma50,
        a.latest_sma200,
        a.close_30d_delta_pct,
        s.weighted_score
    FROM candidates c
    JOIN latest_analysis a ON c.ticker = a.ticker
    JOIN latest_scores s ON c.ticker = s.ticker
    ORDER BY c.ticker, c.options_score DESC
    """
    df = client.query(query).to_dataframe()

    # stringify dates for JSON safety
    for col in ("selection_run_ts", "expiration_date", "fetch_date"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    if not df.empty:
        df["outlook_signal"] = df["weighted_score"].apply(_get_signal_from_score)

    return df

# =========================
# Prompt (tight rules + strict JSON)
# =========================
_PROMPT_TEMPLATE = r"""
You are an options desk analyst. Use ONLY the JSON provided.

Rules (apply even if the JSON is incomplete):
- Directional alignment: If `stock_price_trend_signal` contains "Bullish" and `option_type=="call"`, small upgrade; if it contradicts (e.g., Bearish + call), downgrade.
- Vol regime: If `volatility_comparison_signal=="Expensive"` and we are buying premium, downgrade. If "Cheap", small upgrade.
- Liquidity: If `spread_pct > 10` OR (`open_interest` and `volume` are both small), downgrade for execution risk.
- Decay: If `dte <= 7` AND `theta` is materially negative, downgrade for time-decay risk.
- Breakeven realism: If `breakeven_distance_pct` is large relative to recent moves and trend is not favorable, downgrade; if modest and aligned with trend, upgrade.
- RSI: >70 mention overbought risk; <30 mention oversold bounce potential. Do not upgrade solely on RSI.

Output JSON with exactly these keys:
{
  "setup_quality": "Strong|Fair|Weak",
  "summary": "One sentence referencing 1–2 concrete fields.",
  "drivers": ["liquidity","volatility","alignment","breakeven","decay","momentum"],
  "confidence": 0.0
}

Data:
{contract_data}
"""

# =========================
# Payload builder (NEW fields)
# =========================
def _row_to_llm_payload(row: pd.Series) -> str:
    """Builds the single-contract JSON payload for the LLM from available fields."""
    bid = row.get("bid", None)
    ask = row.get("ask", None)
    last = row.get("last_price", None)
    mid_px = _safe_mid(bid, ask, last)
    spread = _spread_pct(bid, ask, mid_px)

    # Contract IV vs HV(30)
    contract_iv = row.get("implied_volatility")
    hv_30 = row.get("hv_30")
    vol_signal = _get_volatility_signal(contract_iv, hv_30)

    # DTE, Moneyness, Breakeven
    dte = _dte(row.get("expiration_date"), row.get("fetch_date"))
    mny = _moneyness_pct(row.get("option_type"), row.get("underlying_price"), row.get("strike"))
    breakeven_pct = _breakeven_distance_pct(row.get("option_type"), row.get("underlying_price"),
                                            row.get("strike"), mid_px)

    payload = {
        # Stock context
        "ticker": row.get("ticker"),
        "stock_price_trend_signal": row.get("outlook_signal"),
        "rsi": row.get("latest_rsi"),
        "close_30d_delta_pct": row.get("close_30d_delta_pct"),

        # Option context
        "option_type": str(row.get("option_type")).lower() if row.get("option_type") is not None else None,
        "dte": dte,
        "strike": row.get("strike"),
        "underlying_price": row.get("underlying_price"),
        "mid_price": mid_px,
        "spread_pct": spread,
        "open_interest": row.get("open_interest"),
        "volume": row.get("volume"),
        "delta": row.get("delta"),
        "theta": row.get("theta"),

        # Volatility comparison
        "implied_volatility": contract_iv,
        "hv_30": hv_30,
        "volatility_comparison_signal": vol_signal,

        # Edge math
        "moneyness_pct": None if mny is None else mny * 100.0,
        "breakeven_distance_pct": breakeven_pct,
    }

    clean_payload = {k: v for k, v in payload.items() if pd.notna(v)}
    return json.dumps(clean_payload, indent=2)

# =========================
# Single-row processor (updated output fields)
# =========================
def _process_contract(row: pd.Series):
    """Processes a single contract row and returns a dictionary for the BQ table."""
    ticker = row["ticker"]
    csym = row.get("contract_symbol")

    prompt = _PROMPT_TEMPLATE.format(
        example_output=_EXAMPLE_OUTPUT,
        contract_data=_row_to_llm_payload(row),
    )

    try:
        resp = vertex_ai.generate(prompt)

        if not resp or "{" not in resp:
            raise ValueError("Received an empty or invalid response from the AI model.")

        # allow ```json fenced responses
        if resp.strip().startswith("```"):
            match = re.search(r"\{.*\}", resp, re.DOTALL)
            if not match:
                raise ValueError("Could not extract JSON from code block response.")
            resp = match.group(0)

        obj = json.loads(resp)

        quality = obj.get("setup_quality")
        if quality not in ("Strong", "Fair", "Weak"):
            raise ValueError(f"Invalid 'setup_quality' received: {quality}")

        summary = obj.get("summary")
        drivers = obj.get("drivers")
        confidence = obj.get("confidence")

        # recompute vol signal for storage (same logic as payload)
        contract_iv = row.get("implied_volatility")
        hv_30 = row.get("hv_30")
        vol_cmp_signal = _get_volatility_signal(contract_iv, hv_30)

        # Derived again for output stability
        bid = row.get("bid", None); ask = row.get("ask", None); last = row.get("last_price", None)
        mid_px = _safe_mid(bid, ask, last)
        spread = _spread_pct(bid, ask, mid_px)
        dte = _dte(row.get("expiration_date"), row.get("fetch_date"))
        mny = _moneyness_pct(row.get("option_type"), row.get("underlying_price"), row.get("strike"))
        breakeven_pct = _breakeven_distance_pct(row.get("option_type"), row.get("underlying_price"),
                                                row.get("strike"), mid_px)

        return {
            "ticker": ticker,
            "run_date": row.get("fetch_date"),
            "expiration_date": row.get("expiration_date"),
            "contract_symbol": csym,
            "option_type": str(row.get("option_type")).lower() if row.get("option_type") is not None else None,
            "strike_price": row.get("strike"),
            "underlying_price": row.get("underlying_price"),
            "mid_price": mid_px,
            "bid": row.get("bid"),
            "ask": row.get("ask"),
            "spread_pct": spread,
            "open_interest": row.get("open_interest"),
            "volume": row.get("volume"),
            "delta": row.get("delta"),
            "theta": row.get("theta"),
            "implied_volatility": contract_iv,
            "hv_30": hv_30,
            "volatility_comparison_signal": vol_cmp_signal,
            "stock_price_trend_signal": row.get("outlook_signal"),
            "moneyness_pct": None if mny is None else mny * 100.0,
            "breakeven_distance_pct": breakeven_pct,
            "setup_quality_signal": quality,
            "summary": summary,
            "drivers": json.dumps(drivers) if isinstance(drivers, list) else None,
            "confidence": confidence,
        }
    except Exception as e:
        logging.error(f"[{ticker}] Contract {csym} failed: {e}", exc_info=True)
        return None

# =========================
# Load to BQ (unchanged)
# =========================
def _load_df_to_bq(df: pd.DataFrame, table_id: str, project_id: str):
    if df.empty:
        logging.warning("DataFrame is empty. Skipping BigQuery load.")
        return

    client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logging.info(f"Loaded {job.output_rows} rows into BigQuery table: {table_id}")
    except Exception as e:
        logging.error(f"Failed to load DataFrame to {table_id}: {e}", exc_info=True)
        raise

# =========================
# Orchestrator (unchanged)
# =========================
def run_pipeline():
    logging.info("--- Starting Options Analysis Signal Generation ---")
    df = _fetch_candidates_all()
    if df.empty:
        logging.warning("No candidate contracts found. Exiting.")
        return

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_process_contract, row): row.get("contract_symbol") for _, row in df.iterrows()}
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if result:
                    results.append(result)
            except Exception as e:
                logging.error(f"Future for {futures[fut]} failed: {e}", exc_info=True)

    if not results:
        logging.warning("No results were generated after processing. Exiting.")
        return

    output_df = pd.DataFrame(results)
    logging.info(f"Generated {len(output_df)} signals. Loading to BigQuery...")
    _load_df_to_bq(output_df, OUTPUT_TABLE_ID, config.PROJECT_ID)
    logging.info(f"--- Finished. Wrote {len(output_df)} signals to {OUTPUT_TABLE_ID}. ---")
