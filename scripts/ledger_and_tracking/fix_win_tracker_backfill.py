import os
import sys
import logging
from datetime import date, datetime, timedelta, timezone
from google.cloud import bigquery, firestore

# Add local path so we can import from win-tracker
sys.path.append(os.path.join(os.path.dirname(__file__), 'win-tracker'))
from main import get_recent_signals, get_price_history, write_performance_to_bq, write_performance_to_firestore, count_trading_days, MAX_TRADING_DAYS, classify_win, POST_MIN_SCORE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "profitscout-fida8"
DATASET = "profit_scout"

def backfill():
    bq_client = bigquery.Client(project=PROJECT_ID)
    fs_client = firestore.Client(project=PROJECT_ID)

    # Get enriched signals from past 30 calendar days
    signals = get_recent_signals(bq_client, lookback_days=30)
    logger.info(f"Tracking {len(signals)} signals for backfill")

    results = []
    
    import concurrent.futures
    import time
    
    # redefine process_signal because it relies on some scope stuff in track_signal_performance
    def local_process_signal(signal):
        ticker = signal["ticker"]
        signal_date = date.fromisoformat(str(signal["scan_date"]))
        direction = signal["direction"]
        signal_score = signal.get("overnight_score", 0)
        signal_price = float(signal.get("underlying_price", 0) or 0)

        if not signal_price or signal_price <= 0:
            return None

        # How many trading days have passed since signal?
        today = date.today()
        trading_days_elapsed = count_trading_days(signal_date, today)

        if trading_days_elapsed < 1:
            return None  # No trading days yet, skip

        # Get daily prices for the trading window
        prices = get_price_history(ticker, signal_date, days_after=MAX_TRADING_DAYS)
        if not prices:
            return None

        # Calculate returns for each trading day
        returns = []
        for p in prices:
            pct = ((p["close"] - signal_price) / signal_price) * 100
            high_pct = ((p["high"] - signal_price) / signal_price) * 100
            low_pct = ((p["low"] - signal_price) / signal_price) * 100
            returns.append({
                "date": p["date"],
                "close": p["close"],
                "pct_change": round(pct, 2),
                "high_pct": round(high_pct, 2),
                "low_pct": round(low_pct, 2),
            })

        # Peak favorable return within window
        if direction == "BULLISH":
            peak_return = max(r["high_pct"] for r in returns)
        else:
            peak_return = min(r["low_pct"] for r in returns)

        # Current (latest) return
        current_return = returns[-1]["pct_change"]
        current_price = returns[-1]["close"]

        # Classify
        tier = classify_win(peak_return, direction)
        is_win = tier in ("strong", "solid", "directional")

        # Is the signal window complete? (3 trading days have passed)
        is_final = trading_days_elapsed >= MAX_TRADING_DAYS

        return {
            "ticker": ticker,
            "scan_date": signal_date.isoformat(),
            "check_date": today.isoformat(),
            "direction": direction,
            "signal_score": signal_score,
            "signal_price": signal_price,
            "current_price": current_price,
            "pct_change": current_return,
            "peak_return": round(peak_return, 2),
            "trading_days_elapsed": trading_days_elapsed,
            "trading_days_tracked": len(returns),
            "is_win": is_win,
            "tier": tier,
            "is_final": is_final,
            "daily_returns": returns,
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(local_process_signal, s) for s in signals]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
            time.sleep(0.01)

    if results:
        write_performance_to_bq(bq_client, results)
        write_performance_to_firestore(fs_client, results)
        
    logger.info(f"Backfill complete: tracked {len(results)} signals.")

if __name__ == '__main__':
    backfill()
