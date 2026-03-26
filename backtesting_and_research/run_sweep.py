import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import pytz
import pandas_market_calendars as mcal
from google.cloud import bigquery
import time
import math
import itertools

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ID = "profitscout-fida8"
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")

# Set up NYSE calendar
nyse = mcal.get_calendar('NYSE')
est = pytz.timezone('America/New_York')

# --- CONFIGURATION SWEEP DEFINITIONS ---
ENTRY_TIMES = ["09:45", "10:00", "10:30", "11:00", "15:00"]
MAX_HOLD_DAYS = [1, 2, 3]
TARGET_STOPS = [
    (0.20, -0.20), (0.20, -0.25), (0.20, -0.30),
    (0.25, -0.20), (0.25, -0.25), (0.25, -0.30),
    (0.30, -0.20), (0.30, -0.25), (0.30, -0.30),
    (0.40, -0.25), (0.40, -0.40),
    (0.50, -0.25), (0.50, -0.40),
    (0.15, -0.20), (0.15, -0.25),
    (0.35, -0.25), (0.35, -0.30)
]
SCENARIOS = ["Optimistic", "Base", "Stress"]

def get_next_trading_day(base_date: date) -> date:
    end_date = base_date + timedelta(days=7)
    schedule = nyse.schedule(start_date=base_date, end_date=end_date)
    future_dates = [d.date() for d in schedule.index if d.date() > base_date]
    if not future_dates:
        raise ValueError(f"Could not find a trading day after {base_date}")
    return future_dates[0]

def get_trading_day_offset(base_date: date, n_days: int) -> date:
    end_date = base_date + timedelta(days=max(n_days * 2, 14))
    schedule = nyse.schedule(start_date=base_date, end_date=end_date)
    valid_dates = [d.date() for d in schedule.index if d.date() >= base_date]
    if len(valid_dates) < n_days:
        raise ValueError(f"Could not find {n_days} trading days starting from {base_date}")
    return valid_dates[n_days - 1]

def build_polygon_ticker(underlying: str, expiration: date, direction: str, strike: float) -> str:
    try:
        sym = underlying.upper().ljust(6, ' ')[:6].strip()
        exp_str = expiration.strftime("%y%m%d")
        opt_type = "C" if direction.upper() == "BULLISH" else "P"
        strike_int = int(round(strike * 1000))
        strike_str = f"{strike_int:08d}"
        return f"O:{sym}{exp_str}{opt_type}{strike_str}"
    except:
        return None

def fetch_minute_bars(ticker: str, start_date: date, end_date: date) -> list:
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{start_date.isoformat()}/{end_date.isoformat()}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": POLYGON_API_KEY}
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} failed for {ticker}: {e}")
            time.sleep(1)
    return []

def get_data_universe():
    client = bigquery.Client(project=PROJECT_ID)
    query = """
    SELECT ticker, scan_date, recommended_contract, recommended_strike, recommended_expiration, direction, 
           premium_hedge, premium_high_rr, premium_high_atr, premium_score
    FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
    WHERE (is_tradeable = TRUE) OR (premium_score >= 2)
    """
    df = client.query(query).to_dataframe()
    df = df.dropna(subset=['recommended_strike', 'recommended_expiration'])
    
    # Pre-process cohorts
    trades = []
    for _, row in df.iterrows():
        cohorts = []
        if row['premium_hedge'] and row['premium_high_rr']: cohorts.append('HEDGE_HIGH_RR')
        if row['premium_hedge'] and row['premium_high_atr']: cohorts.append('HEDGE_HIGH_ATR')
        if row['premium_score'] >= 2: cohorts.append('SCORE_GTE_2')
        if row['premium_score'] >= 3: cohorts.append('SCORE_GTE_3')
        
        if not cohorts: continue
            
        trade = {
            'ticker': row['ticker'],
            'scan_date': row['scan_date'].date() if isinstance(row['scan_date'], datetime) else row['scan_date'],
            'direction': row['direction'],
            'strike': float(row['recommended_strike']),
            'expiration': row['recommended_expiration'].date() if isinstance(row['recommended_expiration'], pd.Timestamp) or isinstance(row['recommended_expiration'], datetime) else row['recommended_expiration'],
            'cohorts': cohorts
        }
        trades.append(trade)
    return trades

def run_robustness_sweep():
    trades = get_data_universe()
    logger.info(f"Loaded {len(trades)} unique signals for sweep.")
    
    # Pre-fetch all necessary bar data to avoid hitting the API repeatedly in the loops
    # Store by (opt_ticker, start_date) -> bars
    data_cache = {}
    
    for t in trades:
        opt_ticker = build_polygon_ticker(t['ticker'], t['expiration'], t['direction'], t['strike'])
        t['opt_ticker'] = opt_ticker
        if not opt_ticker: continue
        
        try:
            entry_day = get_next_trading_day(t['scan_date'])
            # Max hold is 3 days
            timeout_day_3 = get_trading_day_offset(entry_day, 3)
            timeout_day_2 = get_trading_day_offset(entry_day, 2)
            timeout_day_1 = get_trading_day_offset(entry_day, 1)
            
            t['entry_day'] = entry_day
            t['timeout_days'] = {1: timeout_day_1, 2: timeout_day_2, 3: timeout_day_3}
            
            cache_key = (opt_ticker, entry_day)
            if cache_key not in data_cache:
                logger.info(f"Fetching {opt_ticker} from {entry_day} to {timeout_day_3}")
                bars = fetch_minute_bars(opt_ticker, entry_day, timeout_day_3)
                data_cache[cache_key] = bars
                time.sleep(0.2)
        except Exception as e:
            logger.warning(f"Error preparing {t['ticker']}: {e}")
            t['opt_ticker'] = None

    # Sweep execution
    results = []
    
    total_configs = len(ENTRY_TIMES) * len(TARGET_STOPS) * len(MAX_HOLD_DAYS)
    logger.info(f"Starting sweep of {total_configs} configurations...")
    
    for entry_time in ENTRY_TIMES:
        for target_pct, stop_pct in TARGET_STOPS:
            for max_hold in MAX_HOLD_DAYS:
                
                config_results = []
                
                for t in trades:
                    if not t.get('opt_ticker'): continue
                    bars = data_cache.get((t['opt_ticker'], t['entry_day']), [])
                    
                    # Find entry bar
                    entry_dt = datetime.combine(t['entry_day'], datetime.strptime(entry_time, "%H:%M").time())
                    entry_dt_est = est.localize(entry_dt)
                    entry_ts_ms = int(entry_dt_est.timestamp() * 1000)
                    
                    entry_bar = next((b for b in bars if b['t'] == entry_ts_ms), None)
                    if not entry_bar:
                        entry_bar = next((b for b in bars if b['t'] > entry_ts_ms and datetime.fromtimestamp(b['t']/1000, tz=est).date() == t['entry_day']), None)
                        
                    is_valid = entry_bar is not None and entry_bar.get('v', 0) > 0
                    
                    if not is_valid:
                        # Record invalid liquidity for all cohorts this trade belongs to
                        for cohort in t['cohorts']:
                            for sc in SCENARIOS:
                                config_results.append({
                                    "cohort": cohort,
                                    "scenario": sc,
                                    "valid": False,
                                    "return_pct": 0,
                                    "exit_reason": "invalid_liquidity"
                                })
                        continue
                        
                    raw_entry = entry_bar['c']
                    entry_idx = bars.index(entry_bar)
                    
                    timeout_day = t['timeout_days'][max_hold]
                    timeout_dt = datetime.combine(timeout_day, datetime.strptime("15:59", "%H:%M").time())
                    timeout_dt_est = est.localize(timeout_dt)
                    timeout_ts_ms = int(timeout_dt_est.timestamp() * 1000)
                    
                    for sc_name in SCENARIOS:
                        entry_mult = 1.0
                        timeout_mult = 1.0
                        if sc_name == "Base": entry_mult = 1.02
                        elif sc_name == "Stress": 
                            entry_mult = 1.05
                            timeout_mult = 0.95
                            
                        sc_entry_price = raw_entry * entry_mult
                        target_price = sc_entry_price * (1 + target_pct)
                        # Stop is a loss from entry, so 1 + (-0.25) = 0.75
                        stop_price = sc_entry_price * (1 + stop_pct)
                        
                        exit_reason = "timeout"
                        exit_price = None
                        
                        for j in range(entry_idx + 1, len(bars)):
                            b = bars[j]
                            
                            # Break if past timeout
                            if b['t'] >= timeout_ts_ms:
                                exit_reason = "timeout"
                                exit_price = b['c'] * timeout_mult if sc_name == "Stress" else b['c']
                                break
                                
                            hit_target = b['h'] >= target_price
                            hit_stop = b['l'] <= stop_price
                            
                            if hit_stop and hit_target:
                                exit_reason = "stop"
                                exit_price = min(stop_price, b['c']) if sc_name == "Stress" else stop_price
                                break
                            elif hit_stop:
                                exit_reason = "stop"
                                exit_price = min(stop_price, b['c']) if sc_name == "Stress" else stop_price
                                break
                            elif hit_target:
                                exit_reason = "target"
                                exit_price = target_price
                                break
                                
                        if exit_price is None:
                            # Exhausted data, force timeout
                            last_bar = bars[-1] if len(bars) > entry_idx else entry_bar
                            exit_reason = "timeout"
                            exit_price = last_bar['c'] * timeout_mult if sc_name == "Stress" else last_bar['c']
                            
                        return_pct = (exit_price - sc_entry_price) / sc_entry_price
                        
                        for cohort in t['cohorts']:
                            config_results.append({
                                "cohort": cohort,
                                "scenario": sc_name,
                                "valid": True,
                                "return_pct": return_pct,
                                "exit_reason": exit_reason
                            })
                            
                # Aggregate this configuration
                df_res = pd.DataFrame(config_results)
                if df_res.empty: continue
                
                groups = df_res.groupby(['cohort', 'scenario'])
                for name, group in groups:
                    cohort, sc = name
                    total_trades = len(group)
                    valid_group = group[group['valid'] == True]
                    valid_count = len(valid_group)
                    invalid_count = total_trades - valid_count
                    
                    if valid_count > 0:
                        wins = len(valid_group[valid_group['exit_reason'] == 'target'])
                        stops = len(valid_group[valid_group['exit_reason'] == 'stop'])
                        timeouts = len(valid_group[valid_group['exit_reason'] == 'timeout'])
                        
                        ev = valid_group['return_pct'].mean()
                        med = valid_group['return_pct'].median()
                        
                        # Max streak
                        streak, max_streak = 0, 0
                        for r in valid_group['return_pct']:
                            if r < 0:
                                streak += 1
                                max_streak = max(max_streak, streak)
                            else:
                                streak = 0
                                
                        results.append({
                            "entry_time": entry_time,
                            "target_pct": target_pct,
                            "stop_pct": stop_pct,
                            "max_hold_days": max_hold,
                            "cohort": cohort,
                            "scenario": sc,
                            "sample_size_valid": valid_count,
                            "sample_size_invalid": invalid_count,
                            "win_rate": wins / valid_count,
                            "stop_rate": stops / valid_count,
                            "timeout_rate": timeouts / valid_count,
                            "expected_value": ev,
                            "median_return": med,
                            "max_losing_streak": max_streak
                        })

    final_df = pd.DataFrame(results)
    final_df.to_csv("results/robustness_sweep.csv", index=False)
    logger.info("Sweep complete. Saved to results/robustness_sweep.csv")

if __name__ == "__main__":
    run_robustness_sweep()
