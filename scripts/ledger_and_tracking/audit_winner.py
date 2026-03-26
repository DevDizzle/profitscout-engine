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

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

PROJECT_ID = "profitscout-fida8"
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")

nyse = mcal.get_calendar('NYSE')
est = pytz.timezone('America/New_York')

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
    sym = underlying.upper().ljust(6, ' ')[:6].strip()
    exp_str = expiration.strftime("%y%m%d")
    opt_type = "C" if direction.upper() == "BULLISH" else "P"
    strike_str = f"{int(round(strike * 1000)):08d}"
    return f"O:{sym}{exp_str}{opt_type}{strike_str}"

def fetch_minute_bars(ticker: str, start_date: date, end_date: date) -> list:
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
        except:
            time.sleep(1)
    return []

def audit():
    print("="*80)
    print("AUDIT: 15:00 Entry, SCORE_GTE_2, +40% / -25%, 3-Day Hold (Base Scenario)")
    print("="*80)
    
    client = bigquery.Client(project=PROJECT_ID)
    query = """
    SELECT ticker, scan_date, recommended_contract, recommended_strike, recommended_expiration, direction, premium_score
    FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
    WHERE premium_score >= 2 AND recommended_strike IS NOT NULL AND recommended_expiration IS NOT NULL
    ORDER BY scan_date ASC
    """
    df = client.query(query).to_dataframe()
    print(f"1. INITIAL POPULATION: {len(df)} signals met SCORE_GTE_2 criteria.")
    
    trades = []
    skipped_liquidity = []
    skipped_calendar = []
    
    for _, row in df.iterrows():
        t_date = row['scan_date'].date() if isinstance(row['scan_date'], datetime) else row['scan_date']
        exp_date = row['recommended_expiration'].date() if isinstance(row['recommended_expiration'], pd.Timestamp) or isinstance(row['recommended_expiration'], datetime) else row['recommended_expiration']
        
        opt_ticker = build_polygon_ticker(row['ticker'], exp_date, row['direction'], float(row['recommended_strike']))
        
        try:
            entry_day = get_next_trading_day(t_date)
            timeout_day = get_trading_day_offset(entry_day, 3)
        except:
            skipped_calendar.append(row['ticker'])
            continue
            
        bars = fetch_minute_bars(opt_ticker, entry_day, timeout_day)
        time.sleep(0.2)
        
        entry_dt = datetime.combine(entry_day, datetime.strptime("15:00", "%H:%M").time())
        entry_ts_ms = int(est.localize(entry_dt).timestamp() * 1000)
        timeout_dt = datetime.combine(timeout_day, datetime.strptime("15:59", "%H:%M").time())
        timeout_ts_ms = int(est.localize(timeout_dt).timestamp() * 1000)
        
        entry_bar = next((b for b in bars if b['t'] == entry_ts_ms), None)
        if not entry_bar:
            entry_bar = next((b for b in bars if b['t'] > entry_ts_ms and datetime.fromtimestamp(b['t']/1000, tz=est).date() == entry_day), None)
            
        if not entry_bar or entry_bar.get('v', 0) == 0:
            skipped_liquidity.append(row['ticker'])
            continue
            
        raw_entry = entry_bar['c']
        base_entry = raw_entry * 1.02 # 2% slippage
        target = base_entry * 1.40
        stop = base_entry * 0.75
        
        entry_idx = bars.index(entry_bar)
        exit_reason, exit_price, exit_ts = "timeout", None, None
        
        for j in range(entry_idx + 1, len(bars)):
            b = bars[j]
            b_ts = b['t']
            
            if b_ts >= timeout_ts_ms:
                exit_reason = "timeout"
                exit_price = b['c']
                exit_ts = b_ts
                break
                
            if b['l'] <= stop and b['h'] >= target:
                exit_reason = "stop"
                exit_price = stop
                exit_ts = b_ts
                break
            elif b['l'] <= stop:
                exit_reason = "stop"
                exit_price = stop
                exit_ts = b_ts
                break
            elif b['h'] >= target:
                exit_reason = "target"
                exit_price = target
                exit_ts = b_ts
                break
                
        if exit_price is None:
            last = bars[-1] if len(bars) > entry_idx else entry_bar
            exit_reason = "timeout"
            exit_price = last['c']
            exit_ts = last['t']
            
        ret = (exit_price - base_entry) / base_entry
        
        trades.append({
            'scan_date': t_date,
            'ticker': row['ticker'],
            'opt_ticker': opt_ticker,
            'entry_time': datetime.fromtimestamp(entry_bar['t']/1000, tz=est),
            'exit_time': datetime.fromtimestamp(exit_ts/1000, tz=est),
            'exit_reason': exit_reason,
            'return': ret
        })
        
    print(f"   - Invalid Liquidity Skips: {len(skipped_liquidity)} {skipped_liquidity}")
    print(f"   - Calendar Skips: {len(skipped_calendar)}")
    print(f"2. INCLUSION/EXCLUSION BIAS CHECK: {len(skipped_liquidity)} out of {len(df)} signals skipped due to zero-volume entry bars. This is ~{len(skipped_liquidity)/len(df)*100:.1f}%. The execution simulation safely ignores illiquid contracts, preventing paper-returns on unfillable strikes.")
    
    tdf = pd.DataFrame(trades)
    
    print("\n3. LOOKAHEAD BIAS CHECK:")
    # Ensure no exit time is before entry time
    invalid_time_trades = tdf[tdf['exit_time'] <= tdf['entry_time']]
    if not invalid_time_trades.empty:
        print("   WARNING: Found exits occurring before or at entry time!")
    else:
        print("   PASS: All exit timestamps strictly follow entry timestamps.")
    
    print("\n4. CHRONOLOGICAL SPLIT (HOLDOUT PERFORMANCE):")
    # Sort chronologically and split
    tdf = tdf.sort_values('scan_date')
    midpoint = len(tdf) // 2
    
    h1 = tdf.iloc[:midpoint]
    h2 = tdf.iloc[midpoint:]
    
    def print_stats(name, df_half):
        if len(df_half) == 0: return
        wr = len(df_half[df_half['exit_reason'] == 'target']) / len(df_half)
        ev = df_half['return'].mean()
        print(f"   {name} ({len(df_half)} trades, {df_half['scan_date'].min()} to {df_half['scan_date'].max()})")
        print(f"      Win Rate: {wr*100:.1f}% | EV: {ev*100:.2f}%")
        
    print_stats("First Half (In-Sample)", h1)
    print_stats("Second Half (Out-of-Sample)", h2)
    
    print("\nOverall EV:", f"{tdf['return'].mean()*100:.2f}%")
    print("="*80)

if __name__ == "__main__":
    audit()
