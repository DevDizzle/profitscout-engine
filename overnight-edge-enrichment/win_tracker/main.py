"""
Win Tracker — Signal Performance Tracking
Cloud Function: track_signal_performance (HTTP)
Project: profitscout-fida8

Checks enriched overnight signals from the past 5 trading days against 
current prices. Logs performance and auto-posts wins to X.
"""

import json
import logging
import os
import time
import concurrent.futures
from datetime import date, datetime, timedelta

import functions_framework
from flask import Request
from google.cloud import bigquery, firestore
import requests

logger = logging.getLogger(__name__)

PROJECT_ID = "profitscout-fida8"
DATASET = "profit_scout"
ENRICHED_TABLE = f"{PROJECT_ID}.{DATASET}.overnight_signals_enriched"
PERFORMANCE_TABLE = f"{PROJECT_ID}.{DATASET}.signal_performance"
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()

# X/Twitter credentials (store as secrets in GCP Secret Manager)
X_API_KEY = os.getenv("X_API_KEY", "").strip()
X_API_SECRET = os.getenv("X_API_SECRET", "").strip()
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN", "").strip()
X_ACCESS_SECRET = os.getenv("X_ACCESS_SECRET", "").strip()

# Win thresholds
BULL_WIN_PCT = 5.0    # Stock up 5%+ from signal date = bull win
BEAR_WIN_PCT = -5.0   # Stock down 5%+ from signal date = bear win
POST_MIN_SCORE = 7    # Only post wins for signals that scored 7+


@functions_framework.http
def track_signal_performance(request: Request):
    """Main entry point."""
    bq_client = bigquery.Client(project=PROJECT_ID)
    fs_client = firestore.Client(project=PROJECT_ID)
    
    # Get enriched signals from past 5 trading days
    signals = get_recent_signals(bq_client, lookback_days=5)
    logger.info(f"Tracking {len(signals)} signals from past 5 days")
    
    # Helper to process one signal
    def process_signal(signal):
        ticker = signal["ticker"]
        signal_date = str(signal["scan_date"])
        direction = signal["direction"]
        signal_score = signal.get("overnight_score", 0)
        signal_price = signal.get("underlying_price", 0)
        
        if not signal_price or signal_price <= 0:
            return None
        
        # Get current price
        current_price = get_current_price(ticker)
        if not current_price:
            return None
        
        # Calculate performance
        pct_change = ((current_price - signal_price) / signal_price) * 100
        
        # Determine if it's a win
        is_win = False
        if direction == "BULLISH" and pct_change >= BULL_WIN_PCT:
            is_win = True
        elif direction == "BEARISH" and pct_change <= BEAR_WIN_PCT:
            is_win = True
            
        return {
            "ticker": ticker,
            "scan_date": signal_date,
            "check_date": date.today().isoformat(),
            "direction": direction,
            "signal_score": signal_score,
            "signal_price": signal_price,
            "current_price": current_price,
            "pct_change": round(pct_change, 2),
            "is_win": is_win,
            "days_held": (date.today() - date.fromisoformat(signal_date)).days,
        }

    results = []
    wins = []
    
    # Parallelize fetching with rate limiting handling
    # Reduced workers to avoid rate limits/connection saturation
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_signal, s) for s in signals]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
                if res["is_win"] and res["signal_score"] >= POST_MIN_SCORE:
                    wins.append(res)
            # Small delay to yield CPU/network
            time.sleep(0.01)
    
    # Write all results to BigQuery
    if results:
        write_performance_to_bq(bq_client, results)
    
    # Write to Firestore for webapp display
    if results:
        write_performance_to_firestore(fs_client, results)
    
    # Post wins to X (max 3 per day to avoid spam)
    posted = 0
    # Sort by absolute % change desc
    for win in sorted(wins, key=lambda w: abs(w["pct_change"]), reverse=True)[:3]:
        if post_win_to_x(win):
            posted += 1
    
    summary = {
        "signals_tracked": len(results),
        "wins_found": len(wins),
        "wins_posted_to_x": posted,
        "win_rate": f"{(sum(1 for r in results if r['is_win']) / len(results) * 100):.1f}%" if results else "N/A",
    }
    
    logger.info(f"Performance tracking complete: {json.dumps(summary)}")
    return json.dumps(summary), 200


def get_recent_signals(bq_client, lookback_days=5):
    """Get enriched signals from past N trading days."""
    cutoff = (date.today() - timedelta(days=lookback_days + 2)).isoformat()  # +2 for weekends
    
    query = f"""
    SELECT ticker, scan_date, direction, overnight_score, underlying_price,
           catalyst_type, news_summary, recommended_contract
    FROM `{ENRICHED_TABLE}`
    WHERE scan_date >= '{cutoff}'
      AND overnight_score >= 6
    """
    rows = list(bq_client.query(query).result())
    return [dict(r) for r in rows]


def get_current_price(ticker):
    """Get current/latest price from Polygon with retries."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
    params = {"adjusted": "true", "apiKey": POLYGON_API_KEY}
    
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=10)
            
            if resp.status_code == 429:
                # Rate limited
                time.sleep(1 * (attempt + 1))
                continue
                
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                return results[0].get("c")
            return None
            
        except Exception as e:
            logger.warning(f"Price fetch attempt {attempt+1} failed for {ticker}: {e}")
            time.sleep(0.5 * (attempt + 1))
            
    logger.error(f"Price fetch failed for {ticker} after 3 attempts")
    return None


def write_performance_to_bq(bq_client, results):
    """Write performance results to BigQuery."""
    table_ref = f"{PROJECT_ID}.{DATASET}.signal_performance"
    errors = bq_client.insert_rows_json(table_ref, results)
    if errors:
        logger.error(f"BQ insert errors: {errors}")
    else:
        logger.info(f"Wrote {len(results)} performance rows to BQ")


def write_performance_to_firestore(fs_client, results):
    """Write performance to Firestore for webapp display."""
    batch = fs_client.batch()
    for r in results:
        doc_id = f"{r['scan_date']}_{r['ticker']}"
        ref = fs_client.collection("signal_performance").document(doc_id)
        batch.set(ref, r, merge=True)
    batch.commit()
    logger.info(f"Wrote {len(results)} performance docs to Firestore")


def post_win_to_x(win):
    """Post a win to X/Twitter via Tweepy."""
    # Safety check: ensure credentials exist
    if not all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET]):
        logger.warning("X credentials missing, skipping tweet.")
        return False

    try:
        import tweepy
        
        client = tweepy.Client(
            consumer_key=X_API_KEY,
            consumer_secret=X_API_SECRET,
            access_token=X_ACCESS_TOKEN,
            access_token_secret=X_ACCESS_SECRET,
        )
        
        ticker = win["ticker"]
        score = win["signal_score"]
        direction = win["direction"]
        pct = win["pct_change"]
        days = win["days_held"]
        signal_date = win["scan_date"]
        
        # Format the direction arrow and move
        if direction == "BULLISH":
            arrow = "📈"
            move_text = f"+{abs(pct):.1f}%"
            dir_text = "BULLISH"
        else:
            arrow = "📉"
            move_text = f"-{abs(pct):.1f}%"
            dir_text = "BEARISH"
        
        # Build tweet (no links, human voice, per X content rules)
        tweet = (
            f"{arrow} Scanner called ${ticker} {dir_text} at Score {score} on {signal_date}\\n\\n"
            f"{move_text} in {days} day{'s' if days != 1 else ''}.\\n\\n"
            f"The overnight flow doesnt lie.\\n\\n"
            f"#TheOvernightEdge #OptionsFlow"
        )
        
        response = client.create_tweet(text=tweet)
        logger.info(f"Win posted to X for {ticker}: {response.data['id']}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to post win for {win['ticker']}: {e}")
        return False