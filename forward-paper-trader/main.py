import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import pytz
import pandas_market_calendars as mcal
from google.cloud import bigquery
import yfinance as yf
import time
from flask import Flask, jsonify, request

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "profitscout-fida8"
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "").strip()
FMP_API_KEY = os.environ.get("FMP_API_KEY", "").strip()

nyse = mcal.get_calendar("NYSE")
est = pytz.timezone("America/New_York")

# Config block for Forward Validation Regime (V3)
MIN_PREMIUM_SCORE = 2
MIN_RECOMMENDED_VOLUME = 250
MIN_RECOMMENDED_OI = 500
POLICY_VERSION = "V3_LIQUIDITY_ONLY"
POLICY_GATE = "PREMIUM_GTE_2__VOL_GT_250_OR_OI_GT_500"
LEDGER_TABLE = f"{PROJECT_ID}.profit_scout.forward_paper_ledger_v3"

def get_next_trading_day(base_date: date) -> date:
    end_date = base_date + timedelta(days=7)
    schedule = nyse.schedule(start_date=base_date, end_date=end_date)
    future_dates = [d.date() for d in schedule.index if d.date() > base_date]
    return future_dates[0] if future_dates else None

def get_trading_day_offset(base_date: date, n_days: int) -> date:
    end_date = base_date + timedelta(days=max(n_days * 2, 14))
    schedule = nyse.schedule(start_date=base_date, end_date=end_date)
    valid_dates = [d.date() for d in schedule.index if d.date() >= base_date]
    return valid_dates[n_days - 1]

def build_polygon_ticker(underlying: str, expiration: date, direction: str, strike: float) -> str:
    sym = underlying.upper().ljust(6, " ")[:6].strip()
    exp_str = expiration.strftime("%y%m%d")
    opt_type = "C" if direction.upper() == "BULLISH" else "P"
    strike_str = f"{int(round(strike * 1000)):08d}"
    return f"O:{sym}{exp_str}{opt_type}{strike_str}"

def fetch_minute_bars(ticker: str, start_date: date, end_date: date) -> list:
    if not POLYGON_API_KEY:
        logger.error("POLYGON_API_KEY is not set.")
        return []

    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{start_date.isoformat()}/{end_date.isoformat()}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": POLYGON_API_KEY}
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json().get("results", [])
        except Exception as e:
            logger.warning(f"Polygon API Error: {e}")
            time.sleep(1)
    return []

def get_regime_context(target_date: date):
    """Fetch VIX level and SPY trend state for the given date using FMP API.

    Telemetry only for V3; missing context should not block execution.
    """
    if not FMP_API_KEY:
        logger.warning("FMP_API_KEY is not set; continuing without VIX/SPY telemetry.")
        return None, None
        
    start_date = (target_date - timedelta(days=20)).isoformat()
    end_date = target_date.isoformat()
    
    try:
        # Fetch SPY data
        spy_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/SPY?from={start_date}&to={end_date}&apikey={FMP_API_KEY}"
        spy_res = requests.get(spy_url, timeout=10).json()
        
        # Fetch VIX data
        vix_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/^VIX?from={start_date}&to={end_date}&apikey={FMP_API_KEY}"
        vix_res = requests.get(vix_url, timeout=10).json()
        
        if not spy_res.get("historical") or not vix_res.get("historical"):
            logger.error(f"Missing historical data from FMP API. SPY: {spy_res.get('Error Message', 'No err msg')} VIX: {vix_res.get('Error Message', 'No err msg')}")
            return None, None
            
        spy_hist = sorted(spy_res["historical"], key=lambda x: x["date"])
        vix_hist = sorted(vix_res["historical"], key=lambda x: x["date"])
        
        if not spy_hist or not vix_hist:
             return None, None
             
        # Extract target date or closest prior date
        target_date_str = target_date.isoformat()
        
        # Get closest VIX record
        vix_record = next((r for r in reversed(vix_hist) if r["date"] <= target_date_str), None)
        if not vix_record:
            return None, None
        vix_level = vix_record["close"]
        
        # Get SPY records to calculate SMA
        spy_closes = []
        target_spy_close = None
        for r in spy_hist:
            if r["date"] <= target_date_str:
                spy_closes.append(r["close"])
                if r["date"] == target_date_str or (not target_spy_close and r == spy_hist[-1]):
                    # It will take the exact match, or if the exact match doesn't exist but we hit the end
                    target_spy_close = r["close"]
                    
        # In case the exact date wasn't found in the loop above, just use the last available close
        if target_spy_close is None and spy_closes:
             target_spy_close = spy_closes[-1]
             
        if len(spy_closes) < 10:
             logger.warning(f"Not enough SPY history to calculate SMA_10. Found {len(spy_closes)} days.")
             return None, None
             
        spy_sma = sum(spy_closes[-10:]) / 10
        spy_trend = "BULLISH" if target_spy_close > spy_sma else "BEARISH"
        
        return vix_level, spy_trend
    except Exception as e:
        logger.error(f"Error extracting regime context from FMP for {target_date}: {e}")
        return None, None

def run_forward_paper_trading(target_date: date = None):
    if not target_date:
        # Default to today if not provided
        target_date = datetime.now(est).date()
        
    logger.info(f"Running Forward Paper Trading for signals generated on {target_date}")
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # 1. Fetch Eligible Signals
    # Rules: V3 Logic - premium_score >= 2, (V>=250 or OI>=500)
    query = f"""
    SELECT 
        ticker, scan_date, direction, recommended_contract, recommended_strike, 
        recommended_expiration, recommended_dte, recommended_volume, recommended_oi, 
        recommended_spread_pct, is_premium_signal, premium_score
    FROM `{PROJECT_ID}.profit_scout.overnight_signals_enriched`
    WHERE DATE(scan_date) = "{target_date}"
      AND premium_score >= 2
      AND (recommended_volume > 250 OR recommended_oi > 500)
      AND recommended_strike IS NOT NULL
      AND recommended_expiration IS NOT NULL
    """
    
    df = client.query(query).to_dataframe()
    logger.info(f"Found {len(df)} eligible signals for {target_date}")
    
    if len(df) == 0:
        return True, "No eligible signals found."
        
    # Deduplicate signals: one per ticker per scan_date
    # Priority: 1. highest premium_score, 2. highest volume, 3. fallback deterministically on index
    df = df.sort_values(by=["premium_score", "recommended_volume"], ascending=[False, False])
    
    # Track which ticker/date combos have been seen
    seen_combinations = set()
    deduplicated_rows = []
    
    for _, row in df.iterrows():
        combo_key = (row["ticker"], str(row["scan_date"].date() if isinstance(row["scan_date"], datetime) else row["scan_date"]))
        if combo_key in seen_combinations:
            # We'll process this as a skipped duplicate later, but keep it in the dataframe for ledger logging
            row["is_duplicate"] = True
        else:
            row["is_duplicate"] = False
            seen_combinations.add(combo_key)
        deduplicated_rows.append(row)
        
    df = pd.DataFrame(deduplicated_rows)
        
    # The intended execution day is the next trading day after the scan_date
    entry_day = get_trading_day_offset(target_date, 1) # target_date + 1 trading day
    if entry_day > datetime.now(est).date():
        logger.warning(f"Entry day {entry_day} is in the future. Cannot simulate execution yet.")
        return False, f"Entry day {entry_day} is in the future."
        
    vix_level, spy_trend = get_regime_context(entry_day)

    if vix_level is None:
        logger.info(f"Proceeding without VIX telemetry for {entry_day}.")
    else:
        logger.info(f"Regime telemetry on {entry_day}: VIX = {vix_level:.2f}, SPY Trend = {spy_trend}")
    
    records_to_insert = []
    
    for _, row in df.iterrows():
        is_skipped = False
        skip_reason = None
        
        # 2. Evaluate Skip Conditions

        # Deduplication check
        if row.get("is_duplicate", False):
            is_skipped = True
            skip_reason = "DEDUP_TICKER_DATE_SKIP"
        # Premium Score check
        elif row.get("premium_score", 0) < MIN_PREMIUM_SCORE:
            is_skipped = True
            skip_reason = "LOW_PREMIUM_SCORE_SKIP"

        record = {
            "scan_date": row["scan_date"].date() if isinstance(row["scan_date"], datetime) else row["scan_date"],
            "ticker": row["ticker"],
            "recommended_contract": row["recommended_contract"],
            "direction": row["direction"],
            "is_premium_signal": bool(row["is_premium_signal"]) if pd.notna(row["is_premium_signal"]) else False,
            "premium_score": int(row["premium_score"]) if pd.notna(row["premium_score"]) else 0,
            "policy_version": POLICY_VERSION,
            "policy_gate": POLICY_GATE,
            "is_skipped": is_skipped,
            "skip_reason": skip_reason,
            "VIX_at_entry": float(vix_level) if vix_level is not None else None,
            "SPY_trend_state": spy_trend,
            "recommended_dte": int(row["recommended_dte"]),
            "recommended_volume": int(row["recommended_volume"]),
            "recommended_oi": int(row["recommended_oi"]),
            "recommended_spread_pct": float(row["recommended_spread_pct"]),
            # Execution defaults (will be updated if not skipped)
            "entry_timestamp": None,
            "entry_price": None,
            "target_price": None,
            "stop_price": None,
            "exit_timestamp": None,
            "exit_reason": "SKIPPED" if is_skipped else None,
            "realized_return_pct": None
        }
        
        # 3. Simulate Execution
        if not is_skipped:
            exp_date = row["recommended_expiration"].date() if isinstance(row["recommended_expiration"], pd.Timestamp) or isinstance(row["recommended_expiration"], datetime) else row["recommended_expiration"]
            opt_ticker = build_polygon_ticker(row["ticker"], exp_date, row["direction"], float(row["recommended_strike"]))
            
            timeout_day = get_trading_day_offset(entry_day, 3)
            
            bars = fetch_minute_bars(opt_ticker, entry_day, timeout_day)
            time.sleep(0.2)
            
            entry_dt = datetime.combine(entry_day, datetime.strptime("15:00", "%H:%M").time())
            entry_ts_ms = int(est.localize(entry_dt).timestamp() * 1000)
            timeout_dt = datetime.combine(timeout_day, datetime.strptime("15:59", "%H:%M").time())
            timeout_ts_ms = int(est.localize(timeout_dt).timestamp() * 1000)
            
            entry_bar = next((b for b in bars if b["t"] == entry_ts_ms), None)
            if not entry_bar:
                entry_bar = next((b for b in bars if b["t"] > entry_ts_ms and datetime.fromtimestamp(b["t"]/1000, tz=est).date() == entry_day), None)
                
            if not entry_bar or entry_bar.get("v", 0) == 0:
                record["exit_reason"] = "INVALID_LIQUIDITY"
            else:
                base_entry = entry_bar["c"] * 1.02 # 2% Base Slippage
                target = base_entry * 1.40
                stop = base_entry * 0.75
                
                record["entry_timestamp"] = datetime.fromtimestamp(entry_bar["t"]/1000, tz=est).isoformat()
                record["entry_price"] = base_entry
                record["target_price"] = target
                record["stop_price"] = stop
                
                entry_idx = bars.index(entry_bar)
                exit_reason = "TIMEOUT"
                exit_price = None
                exit_ts = None
                
                for j in range(entry_idx + 1, len(bars)):
                    b = bars[j]
                    b_ts = b["t"]
                    
                    if b_ts >= timeout_ts_ms:
                        exit_reason = "TIMEOUT"
                        exit_price = b["c"]
                        exit_ts = b_ts
                        break
                        
                    if b["l"] <= stop and b["h"] >= target:
                        exit_reason = "STOP"
                        exit_price = stop
                        exit_ts = b_ts
                        break
                    elif b["l"] <= stop:
                        exit_reason = "STOP"
                        exit_price = stop
                        exit_ts = b_ts
                        break
                    elif b["h"] >= target:
                        exit_reason = "TARGET"
                        exit_price = target
                        exit_ts = b_ts
                        break
                        
                if exit_price is None:
                    last = bars[-1] if len(bars) > entry_idx else entry_bar
                    exit_reason = "TIMEOUT"
                    exit_price = last["c"]
                    exit_ts = last["t"]
                    
                ret = (exit_price - base_entry) / base_entry
                
                record["exit_reason"] = exit_reason
                record["exit_timestamp"] = datetime.fromtimestamp(exit_ts/1000, tz=est).isoformat()
                record["realized_return_pct"] = float(ret)
                
        records_to_insert.append(record)
        
    # 4. Write to BigQuery
    if records_to_insert:
        table_ref = LEDGER_TABLE
        # Convert date to string for BQ insert
        for r in records_to_insert:
            if isinstance(r["scan_date"], date):
                r["scan_date"] = r["scan_date"].isoformat()
        
        errors = client.insert_rows_json(table_ref, records_to_insert)
        if errors:
            logger.error(f"Errors inserting rows: {errors}")
            return False, f"Errors inserting rows: {errors}"
        else:
            logger.info(f"Successfully inserted {len(records_to_insert)} records into {table_ref}")
            return True, f"Successfully inserted {len(records_to_insert)} records."
    return True, "No records to insert."

@app.route("/", methods=["GET", "POST"])
def trigger_paper_trading():
    try:
        # Check if a specific target date is passed via JSON payload
        req_data = request.get_json(silent=True)
        target_date_str = req_data.get("target_date") if req_data else None
        
        if target_date_str:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
        else:
            # Default logic: get the most recent trading day relative to 7 days ago, offset by 5...
            # Actually, standard behavior was to run for yesterday's scan date by default (since execution is today)
            yesterday = datetime.now(est).date() - timedelta(days=1)
            # Find the last valid scan date
            target_date = get_trading_day_offset(datetime.now(est).date() - timedelta(days=7), 5)
            
        success, msg = run_forward_paper_trading(target_date)
        if success:
            return jsonify({"status": "success", "message": msg}), 200
        else:
            return jsonify({"status": "error", "message": msg}), 500
    except Exception as e:
        logger.error(f"Error in paper trading endpoint: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

