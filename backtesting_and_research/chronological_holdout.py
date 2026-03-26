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

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "profitscout-fida8"
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")

nyse = mcal.get_calendar("NYSE")
est = pytz.timezone("America/New_York")

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

def run_execution_test():
    print("="*80)
    print("CHRONOLOGICAL HOLDOUT TEST ON GATED COHORT")
    print("Cohort: SCORE_GTE_2 | Gate: V>=100 OR OI>=250")
    print("Execution: 15:00 ET Entry | +40% Target | -25% Stop | 3-Day Hold (Base Scenario +2% slippage)")
    print("="*80)
    
    client = bigquery.Client(project=PROJECT_ID)
    query = """
    SELECT ticker, scan_date, recommended_contract, recommended_strike, recommended_expiration, direction, premium_score, recommended_volume, recommended_oi
    FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
    WHERE premium_score >= 2 
      AND recommended_strike IS NOT NULL 
      AND recommended_expiration IS NOT NULL
      AND (recommended_volume >= 100 OR recommended_oi >= 250)
    ORDER BY scan_date ASC
    """
    df = client.query(query).to_dataframe()
    print(f"Total Signals passing Gate: {len(df)}")
    
    trades = []
    
    for _, row in df.iterrows():
        t_date = row["scan_date"].date() if isinstance(row["scan_date"], datetime) else row["scan_date"]
        exp_date = row["recommended_expiration"].date() if isinstance(row["recommended_expiration"], pd.Timestamp) or isinstance(row["recommended_expiration"], datetime) else row["recommended_expiration"]
        opt_ticker = build_polygon_ticker(row["ticker"], exp_date, row["direction"], float(row["recommended_strike"]))
        
        try:
            entry_day = get_next_trading_day(t_date)
            timeout_day = get_trading_day_offset(entry_day, 3)
        except:
            continue
            
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
            continue
            
        base_entry = entry_bar["c"] * 1.02 # 2% Base Slippage
        target = base_entry * 1.40
        stop = base_entry * 0.75
        
        entry_idx = bars.index(entry_bar)
        exit_reason, exit_price, exit_ts = "timeout", None, None
        
        for j in range(entry_idx + 1, len(bars)):
            b = bars[j]
            b_ts = b["t"]
            
            if b_ts >= timeout_ts_ms:
                exit_reason = "timeout"
                exit_price = b["c"]
                exit_ts = b_ts
                break
                
            if b["l"] <= stop and b["h"] >= target:
                exit_reason = "stop"
                exit_price = stop
                exit_ts = b_ts
                break
            elif b["l"] <= stop:
                exit_reason = "stop"
                exit_price = stop
                exit_ts = b_ts
                break
            elif b["h"] >= target:
                exit_reason = "target"
                exit_price = target
                exit_ts = b_ts
                break
                
        if exit_price is None:
            last = bars[-1] if len(bars) > entry_idx else entry_bar
            exit_reason = "timeout"
            exit_price = last["c"]
            exit_ts = last["t"]
            
        ret = (exit_price - base_entry) / base_entry
        
        trades.append({
            "scan_date": t_date,
            "ticker": row["ticker"],
            "exit_reason": exit_reason,
            "return_pct": ret
        })
        
    tdf = pd.DataFrame(trades)
    
    if len(tdf) > 0:
        tdf = tdf.sort_values("scan_date")
        midpoint = len(tdf) // 2
        
        h1 = tdf.iloc[:midpoint]
        h2 = tdf.iloc[midpoint:]
        
        def print_stats(name, df_half):
            if len(df_half) == 0: return
            wr = len(df_half[df_half["exit_reason"] == "target"]) / len(df_half)
            ev = df_half["return_pct"].mean()
            print(f"   {name} ({len(df_half)} trades, {df_half['scan_date'].min()} to {df_half['scan_date'].max()})")
            print(f"      Win Rate: {wr*100:.1f}% | EV: {ev*100:.2f}%")
            
        print_stats("First Half (In-Sample)", h1)
        print_stats("Second Half (Out-of-Sample)", h2)
        
        print(f"\nOverall Valid Executable Trades: {len(tdf)}")
        print(f"Overall EV: {tdf['return_pct'].mean()*100:.2f}%")
    else:
        print("No valid trades found.")

if __name__ == "__main__":
    run_execution_test()

