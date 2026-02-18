"""
Win Tracker — Signal Performance Tracking
Cloud Run service: win-tracker
Project: profitscout-fida8

Checks enriched overnight signals against actual price movement
over a 3-TRADING-DAY window. Tracks peak return, classifies by tier,
and posts strong wins to X.
"""

import json
import logging
import os
import time
import concurrent.futures
from datetime import date, datetime, timedelta

from flask import Flask, jsonify
from google.cloud import bigquery, firestore
import requests

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "profitscout-fida8"
DATASET = "profit_scout"
ENRICHED_TABLE = f"{PROJECT_ID}.{DATASET}.overnight_signals_enriched"
PERFORMANCE_TABLE = f"{PROJECT_ID}.{DATASET}.signal_performance"
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()

# X/Twitter credentials
X_API_KEY = os.getenv("X_API_KEY", "").strip()
X_API_SECRET = os.getenv("X_API_SECRET", "").strip()
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN", "").strip()
X_ACCESS_SECRET = os.getenv("X_ACCESS_SECRET", "").strip()

# Win tier thresholds (based on peak return in right direction)
TIER_NO_DECISION = 1.0    # < 1% = too small to call
TIER_DIRECTIONAL = 1.0    # >= 1% = directional win
TIER_SOLID = 3.0           # >= 3% = solid win
TIER_STRONG = 5.0          # >= 5% = strong win

POST_MIN_SCORE = 7         # Only post wins for signals scored 7+
POST_MIN_TIER = "strong"   # Only post strong wins to X
MAX_TRADING_DAYS = 3       # Track over 3 trading day window

# US Market holidays 2026 (add as needed)
HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
    "2026-11-26", "2026-12-25",
}


def is_trading_day(d: date) -> bool:
    """Check if a date is a trading day (not weekend, not holiday)."""
    if d.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    if d.isoformat() in HOLIDAYS_2026:
        return False
    return True


def count_trading_days(from_date: date, to_date: date) -> int:
    """Count trading days between two dates (exclusive of from_date)."""
    count = 0
    current = from_date + timedelta(days=1)
    while current <= to_date:
        if is_trading_day(current):
            count += 1
        current += timedelta(days=1)
    return count


def get_trading_days_after(from_date: date, n_days: int) -> list[date]:
    """Get the next N trading days after from_date."""
    days = []
    current = from_date + timedelta(days=1)
    while len(days) < n_days:
        if is_trading_day(current):
            days.append(current)
        current += timedelta(days=1)
        if current > from_date + timedelta(days=30):  # safety limit
            break
    return days


def classify_win(peak_return_pct: float, direction: str) -> str:
    """
    Classify signal result based on peak favorable return within window.
    Returns: 'strong', 'solid', 'directional', 'no_decision', 'loss'
    """
    # Peak return should be in the right direction
    if direction == "BULLISH":
        favorable = peak_return_pct  # positive is good
    else:
        favorable = -peak_return_pct  # negative price move is good for bears

    if favorable >= TIER_STRONG:
        return "strong"
    elif favorable >= TIER_SOLID:
        return "solid"
    elif favorable >= TIER_DIRECTIONAL:
        return "directional"
    elif favorable >= 0:
        return "no_decision"
    else:
        return "loss"


@app.route("/", methods=["GET", "POST"])
def track_signal_performance():
    """Main entry point."""
    bq_client = bigquery.Client(project=PROJECT_ID)
    fs_client = firestore.Client(project=PROJECT_ID)

    # Get enriched signals from past 7 calendar days (covers weekends + holidays)
    signals = get_recent_signals(bq_client, lookback_days=7)
    logger.info(f"Tracking {len(signals)} signals")

    def process_signal(signal):
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
            returns.append({
                "date": p["date"],
                "close": p["close"],
                "pct_change": round(pct, 2),
            })

        # Peak favorable return within window
        if direction == "BULLISH":
            peak_return = max(r["pct_change"] for r in returns)
        else:
            peak_return = min(r["pct_change"] for r in returns)

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

    results = []
    strong_wins = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_signal, s) for s in signals]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
                # Only collect strong wins for X posting
                if (res["tier"] == "strong" and
                    res["signal_score"] >= POST_MIN_SCORE and
                    res["is_final"]):  # Only post after window completes
                    strong_wins.append(res)
            time.sleep(0.01)

    # Write all results to BigQuery + Firestore
    if results:
        write_performance_to_bq(bq_client, results)
        write_performance_to_firestore(fs_client, results)

    # Post strong wins to X (max 3 per day)
    posted = 0
    for win in sorted(strong_wins, key=lambda w: abs(w["peak_return"]), reverse=True)[:3]:
        if post_win_to_x(win):
            posted += 1

    # Tally by tier
    tier_counts = {}
    for r in results:
        t = r["tier"]
        tier_counts[t] = tier_counts.get(t, 0) + 1

    summary = {
        "signals_tracked": len(results),
        "tiers": tier_counts,
        "strong_wins_posted": posted,
        "win_rate": f"{(sum(1 for r in results if r['is_win']) / len(results) * 100):.1f}%" if results else "N/A",
    }

    logger.info(f"Performance tracking complete: {json.dumps(summary)}")
    return jsonify(summary), 200


def get_recent_signals(bq_client, lookback_days=7):
    """Get enriched signals from past N days."""
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

    query = f"""
    SELECT ticker, scan_date, direction, overnight_score, underlying_price,
           catalyst_type, news_summary, recommended_contract
    FROM `{ENRICHED_TABLE}`
    WHERE scan_date >= '{cutoff}'
      AND overnight_score >= 6
    """
    rows = list(bq_client.query(query).result())
    return [dict(r) for r in rows]


def get_price_history(ticker: str, signal_date: date, days_after: int = 3) -> list[dict]:
    """
    Get daily closing prices for trading days after signal_date.
    Returns up to `days_after` trading days of price data.
    """
    # Calculate end date (signal_date + enough calendar days to cover trading days)
    end_date = signal_date + timedelta(days=days_after * 2 + 5)  # generous buffer
    if end_date > date.today():
        end_date = date.today()

    start_date = signal_date + timedelta(days=1)  # day after signal

    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date.isoformat()}/{end_date.isoformat()}"
    params = {"adjusted": "true", "sort": "asc", "apiKey": POLYGON_API_KEY}

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=10)

            if resp.status_code == 429:
                time.sleep(1 * (attempt + 1))
                continue

            resp.raise_for_status()
            bars = resp.json().get("results", [])

            if not bars:
                return []

            # Convert to clean list, limit to MAX_TRADING_DAYS trading days
            prices = []
            for bar in bars:
                bar_date = datetime.fromtimestamp(bar["t"] / 1000).strftime("%Y-%m-%d")
                if is_trading_day(date.fromisoformat(bar_date)):
                    prices.append({
                        "date": bar_date,
                        "close": bar["c"],
                    })
                    if len(prices) >= days_after:
                        break

            return prices

        except Exception as e:
            logger.warning(f"Price history attempt {attempt+1} failed for {ticker}: {e}")
            time.sleep(0.5 * (attempt + 1))

    logger.error(f"Price history failed for {ticker} after 3 attempts")
    return []


def write_performance_to_bq(bq_client, results):
    """Write performance results to BigQuery."""
    # Flatten for BQ (remove daily_returns nested field)
    bq_rows = []
    for r in results:
        row = {k: v for k, v in r.items() if k != "daily_returns"}
        bq_rows.append(row)

    table_ref = f"{PROJECT_ID}.{DATASET}.signal_performance"

    # Delete existing rows for these signals to avoid duplicates
    scan_dates = list(set(r["scan_date"] for r in results))
    for sd in scan_dates:
        try:
            delete_q = f"DELETE FROM `{table_ref}` WHERE scan_date = '{sd}'"
            bq_client.query(delete_q).result()
        except Exception as e:
            logger.warning(f"BQ delete failed for {sd}: {e}")

    errors = bq_client.insert_rows_json(table_ref, bq_rows)
    if errors:
        logger.error(f"BQ insert errors: {errors}")
    else:
        logger.info(f"Wrote {len(bq_rows)} performance rows to BQ")


def write_performance_to_firestore(fs_client, results):
    """Write performance to Firestore for webapp display."""
    batch = fs_client.batch()
    count = 0
    for r in results:
        doc_id = f"{r['scan_date']}_{r['ticker']}"
        ref = fs_client.collection("signal_performance").document(doc_id)
        # Store clean version (no daily_returns in Firestore to save space)
        doc_data = {k: v for k, v in r.items() if k != "daily_returns"}
        batch.set(ref, doc_data, merge=True)
        count += 1
        if count % 400 == 0:
            batch.commit()
            batch = fs_client.batch()
    batch.commit()
    logger.info(f"Wrote {count} performance docs to Firestore")


def post_win_to_x(win):
    """Post a strong win to X/Twitter via Tweepy."""
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
        peak = win["peak_return"]
        trading_days = win["trading_days_tracked"]
        signal_date = win["scan_date"]

        if direction == "BULLISH":
            arrow = "📈"
            move_text = f"+{abs(peak):.1f}%"
            dir_text = "BULLISH"
        else:
            arrow = "📉"
            move_text = f"-{abs(peak):.1f}%"
            dir_text = "BEARISH"

        import random
        taglines = [
            "The overnight flow don't lie.",
            "Institutional money moves before you wake up.",
            "While you were sleeping, smart money was positioning.",
            "We scan 5,000+ tickers overnight so you don't have to.",
            "The flow showed up. The move followed.",
        ]
        tagline = random.choice(taglines)

        day_text = f"{trading_days} trading day{'s' if trading_days != 1 else ''}"

        tweet = (
            f"{arrow} Scanner called ${ticker} {dir_text} at Score {score} on {signal_date}\n\n"
            f"{move_text} peak move in {day_text}.\n\n"
            f"{tagline}\n\n"
            f"#TheOvernightEdge #OptionsFlow"
        )

        response = client.create_tweet(text=tweet)
        logger.info(f"Strong win posted to X for {ticker}: {response.data['id']}")
        return True

    except Exception as e:
        logger.error(f"Failed to post win for {win['ticker']}: {e}")
        return False


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
