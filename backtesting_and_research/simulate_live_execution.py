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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ID = "profitscout-fida8"
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")

# Set up NYSE calendar
nyse = mcal.get_calendar('NYSE')

def get_next_trading_day(base_date: date) -> date:
    """Find the next valid trading session after the base_date."""
    # Look ahead up to 7 days to find the next valid session
    end_date = base_date + timedelta(days=7)
    schedule = nyse.schedule(start_date=base_date, end_date=end_date)
    # Filter for dates strictly greater than base_date
    future_dates = [d.date() for d in schedule.index if d.date() > base_date]
    if not future_dates:
        raise ValueError(f"Could not find a trading day after {base_date}")
    return future_dates[0]

def get_trading_day_offset(base_date: date, n_days: int) -> date:
    """Find the Nth trading session starting from the base_date (where base_date is day 1)."""
    # Look ahead to ensure we have enough days
    end_date = base_date + timedelta(days=max(n_days * 2, 14))
    schedule = nyse.schedule(start_date=base_date, end_date=end_date)
    valid_dates = [d.date() for d in schedule.index if d.date() >= base_date]
    if len(valid_dates) < n_days:
        raise ValueError(f"Could not find {n_days} trading days starting from {base_date}")
    # 0-indexed list, so day 3 is index 2
    return valid_dates[n_days - 1]

def build_polygon_ticker(underlying: str, expiration: date, direction: str, strike: float) -> str:
    """Builds the 21-character Polygon options ticker."""
    try:
        # e.g. O:SPY241220C00500000
        sym = underlying.upper().ljust(6, ' ')[:6].strip() # Up to 6 chars, no padding needed if using standard construction but let's be safe
        exp_str = expiration.strftime("%y%m%d")
        opt_type = "C" if direction.upper() == "BULLISH" else "P"
        # Strike is 8 digits, price * 1000
        strike_int = int(round(strike * 1000))
        strike_str = f"{strike_int:08d}"
        return f"O:{sym}{exp_str}{opt_type}{strike_str}"
    except Exception as e:
        logger.error(f"Failed to build ticker for {underlying}: {e}")
        return None

def fetch_minute_bars(ticker: str, start_date: date, end_date: date) -> list:
    """Fetch 1-minute bars from Polygon for the given window."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{start_date.isoformat()}/{end_date.isoformat()}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": POLYGON_API_KEY
    }
    
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

def run_simulation():
    client = bigquery.Client(project=PROJECT_ID)
    
    query = """
    SELECT ticker, scan_date, recommended_contract, recommended_strike, recommended_expiration, direction, premium_hedge, premium_high_rr, premium_high_atr, underlying_price
    FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
    WHERE is_tradeable = TRUE
    """
    df = client.query(query).to_dataframe()
    logger.info(f"Found {len(df)} tradeable signals to simulate.")
    
    ledger = []
    
    # Filter out rows with missing critical contract data
    df = df.dropna(subset=['recommended_strike', 'recommended_expiration'])
    logger.info(f"{len(df)} signals have complete contract data.")

    for _, row in df.iterrows():
        base_ticker = row['ticker']
        scan_date = row['scan_date']
        if isinstance(scan_date, datetime):
            scan_date = scan_date.date()
            
        direction = row['direction']
        strike = float(row['recommended_strike'])
        expiration = row['recommended_expiration']
        if isinstance(expiration, pd.Timestamp) or isinstance(expiration, datetime):
            expiration = expiration.date()
            
        # Determine combo type
        combo = "UNKNOWN"
        if row['premium_hedge'] and row['premium_high_rr'] and row['premium_high_atr']:
            combo = "HEDGE_HIGH_RR_ATR"
        elif row['premium_hedge'] and row['premium_high_rr']:
            combo = "HEDGE_HIGH_RR"
        elif row['premium_hedge'] and row['premium_high_atr']:
            combo = "HEDGE_HIGH_ATR"
            
        opt_ticker = build_polygon_ticker(base_ticker, expiration, direction, strike)
        if not opt_ticker:
            continue
            
        # Calendar logic
        try:
            entry_day = get_next_trading_day(scan_date)
            timeout_day = get_trading_day_offset(entry_day, 3)
            # DTE at entry
            dte = (expiration - entry_day).days
        except Exception as e:
            logger.warning(f"Calendar error for {base_ticker} on {scan_date}: {e}")
            continue
            
        # Fetch data
        logger.info(f"Fetching {opt_ticker} from {entry_day} to {timeout_day}...")
        bars = fetch_minute_bars(opt_ticker, entry_day, timeout_day)
        time.sleep(0.5) # rate limit
        
        # Process bars to find entry
        # Expected entry time: 09:45 AM ET on entry_day
        # Convert to millisecond timestamp (Polygon format)
        est = pytz.timezone('America/New_York')
        entry_dt = datetime.combine(entry_day, datetime.strptime("09:45", "%H:%M").time())
        entry_dt_est = est.localize(entry_dt)
        entry_ts_ms = int(entry_dt_est.timestamp() * 1000)
        
        timeout_dt = datetime.combine(timeout_day, datetime.strptime("15:59", "%H:%M").time())
        timeout_dt_est = est.localize(timeout_dt)
        timeout_ts_ms = int(timeout_dt_est.timestamp() * 1000)
        
        entry_bar = None
        entry_idx = -1
        
        for i, b in enumerate(bars):
            # Polygon uses start of minute timestamp. We look for exactly 9:45.
            if b['t'] == entry_ts_ms:
                entry_bar = b
                entry_idx = i
                break
                
        # If no exact 9:45 bar, try to find the first bar AFTER 9:45 on the entry day
        if not entry_bar:
            for i, b in enumerate(bars):
                bar_dt = datetime.fromtimestamp(b['t']/1000, tz=est)
                if bar_dt.date() == entry_day and bar_dt.time() >= datetime.strptime("09:45", "%H:%M").time():
                    entry_bar = b
                    entry_idx = i
                    break
        
        # Base metadata for ledger rows
        trade_meta = {
            "ticker": base_ticker,
            "scan_date": scan_date.isoformat(),
            "entry_session_date": entry_day.isoformat(),
            "timeout_session_date": timeout_day.isoformat(),
            "combo_type": combo,
            "direction": direction,
            "options_ticker": opt_ticker,
            "dte_at_entry": dte,
            "bars_available": len(bars)
        }
        
        if not entry_bar or entry_bar.get('v', 0) == 0:
            # Invalid liquidity
            for sc in ["Optimistic", "Base", "Stress"]:
                row_data = trade_meta.copy()
                row_data.update({
                    "scenario": sc,
                    "liquidity_flag": "Invalid",
                    "entry_timestamp": None,
                    "entry_bar_volume": 0,
                    "entry_price": None,
                    "target_price": None,
                    "stop_price": None,
                    "exit_timestamp": None,
                    "exit_reason": "invalid_liquidity",
                    "bars_to_exit": 0,
                    "exit_price": None,
                    "return_pct": 0.0
                })
                ledger.append(row_data)
            continue
            
        raw_entry = entry_bar['c']
        entry_vol = entry_bar['v']
        
        # Scenario loop
        scenarios = [
            ("Optimistic", raw_entry, 1.0),
            ("Base", raw_entry * 1.02, 1.0), # 2% slippage on entry
            ("Stress", raw_entry * 1.05, 0.95) # 5% slippage on entry, 5% on timeout exit
        ]
        
        for sc_name, sc_entry_price, sc_timeout_mult in scenarios:
            row_data = trade_meta.copy()
            row_data["scenario"] = sc_name
            row_data["liquidity_flag"] = "Valid"
            row_data["entry_timestamp"] = datetime.fromtimestamp(entry_bar['t']/1000, tz=est).isoformat()
            row_data["entry_bar_volume"] = entry_vol
            row_data["entry_price"] = sc_entry_price
            
            target_price = sc_entry_price * 1.40
            stop_price = sc_entry_price * 0.75
            
            row_data["target_price"] = target_price
            row_data["stop_price"] = stop_price
            
            exit_reason = "timeout"
            exit_price = None
            exit_ts = None
            bars_to_exit = 0
            
            # Simulate minute by minute
            for j in range(entry_idx + 1, len(bars)):
                b = bars[j]
                b_high = b['h']
                b_low = b['l']
                b_close = b['c']
                b_ts = b['t']
                bars_to_exit += 1
                
                hit_target = b_high >= target_price
                hit_stop = b_low <= stop_price
                
                if hit_stop and hit_target:
                    # Conservative rule: stop hit first
                    exit_reason = "stop"
                    exit_ts = datetime.fromtimestamp(b_ts/1000, tz=est).isoformat()
                    if sc_name == "Stress":
                        # Adverse fill realism: worse of stop price or minute close
                        exit_price = min(stop_price, b_close)
                    else:
                        exit_price = stop_price
                    break
                elif hit_stop:
                    exit_reason = "stop"
                    exit_ts = datetime.fromtimestamp(b_ts/1000, tz=est).isoformat()
                    if sc_name == "Stress":
                        exit_price = min(stop_price, b_close)
                    else:
                        exit_price = stop_price
                    break
                elif hit_target:
                    exit_reason = "target"
                    exit_ts = datetime.fromtimestamp(b_ts/1000, tz=est).isoformat()
                    exit_price = target_price
                    break
                    
                # Check for timeout (end of regular hours on day 3)
                if b_ts >= timeout_ts_ms:
                    exit_reason = "timeout"
                    exit_ts = datetime.fromtimestamp(b_ts/1000, tz=est).isoformat()
                    if sc_name == "Stress":
                        exit_price = b_close * sc_timeout_mult # penalize timeout exit
                    else:
                        exit_price = b_close
                    break
            
            # If we exhausted bars without hitting limits or timeout time 
            # (e.g., partial day data), default to timeout at last bar
            if exit_price is None and len(bars) > entry_idx:
                last_bar = bars[-1]
                exit_reason = "timeout"
                exit_ts = datetime.fromtimestamp(last_bar['t']/1000, tz=est).isoformat()
                if sc_name == "Stress":
                    exit_price = last_bar['c'] * sc_timeout_mult
                else:
                    exit_price = last_bar['c']
                    
            if exit_price is not None:
                return_pct = (exit_price - sc_entry_price) / sc_entry_price
            else:
                return_pct = 0.0
                
            row_data["exit_timestamp"] = exit_ts
            row_data["exit_reason"] = exit_reason
            row_data["bars_to_exit"] = bars_to_exit
            row_data["exit_price"] = exit_price
            row_data["return_pct"] = return_pct
            
            ledger.append(row_data)

    # Convert to DataFrame
    res_df = pd.DataFrame(ledger)
    res_df.to_csv("trade_ledger.csv", index=False)
    logger.info("Saved trade_ledger.csv")
    
    # Generate System Report
    print("\n" + "="*80)
    print("PROTOCOL-ACCURATE INTRADAY EXECUTION BACKTEST REPORT")
    print("="*80)
    
    valid_df = res_df[res_df['liquidity_flag'] == 'Valid']
    invalid_count = len(res_df[res_df['scenario'] == 'Base']) - len(valid_df[valid_df['scenario'] == 'Base'])
    
    print(f"Total Signals Evaluated: {len(res_df) // 3}")
    print(f"Invalid Liquidity Skips: {invalid_count}")
    print(f"Valid Executable Trades: {len(valid_df) // 3}")
    print("-"*80)
    
    if not valid_df.empty:
        # Group by Scenario and Combo
        groups = valid_df.groupby(['scenario', 'combo_type'])
        
        for name, group in groups:
            sc, combo = name
            total = len(group)
            wins = len(group[group['exit_reason'] == 'target'])
            losses = len(group[group['exit_reason'] == 'stop'])
            timeouts = len(group[group['exit_reason'] == 'timeout'])
            
            win_rate = wins / total
            loss_rate = losses / total
            timeout_rate = timeouts / total
            
            # EV calculation (Average of all return percentages)
            ev = group['return_pct'].mean()
            median_ret = group['return_pct'].median()
            
            # Max losing streak
            streak = 0
            max_streak = 0
            for r in group['return_pct']:
                if r < 0:
                    streak += 1
                    max_streak = max(max_streak, streak)
                else:
                    streak = 0
                    
            print(f"SCENARIO: {sc:12} | COMBO: {combo}")
            print(f"  Trades: {total}")
            print(f"  Win Rate: {win_rate*100:.1f}% | Loss Rate: {loss_rate*100:.1f}% | Timeout Rate: {timeout_rate*100:.1f}%")
            print(f"  Expected Value (EV): {ev*100:.2f}% per trade")
            print(f"  Median Return:       {median_ret*100:.2f}%")
            print(f"  Max Losing Streak:   {max_streak}")
            print("-"*80)

if __name__ == "__main__":
    run_simulation()
