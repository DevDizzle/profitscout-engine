import os
import requests
from datetime import datetime, timedelta

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")

def get_vix(date_str):
    url = f"https://api.polygon.io/v2/aggs/ticker/I:VIX/range/1/day/{date_str}/{date_str}?adjusted=true&sort=asc&apiKey={POLYGON_API_KEY}"
    res = requests.get(url).json()
    if res.get("results"):
        return res["results"][0]["c"]
    return None

def get_spy_sma(date_str):
    # Just to test we can get SPY daily data via Polygon
    url = f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/2026-03-01/{date_str}?adjusted=true&sort=asc&apiKey={POLYGON_API_KEY}"
    res = requests.get(url).json()
    if res.get("results"):
        closes = [r["c"] for r in res["results"]]
        if len(closes) >= 10:
            sma = sum(closes[-10:]) / 10
            return closes[-1], sma
    return None, None

print(f"VIX on 2026-03-16: {get_vix('2026-03-16')}")
print(f"SPY Data up to 2026-03-16: {get_spy_sma('2026-03-16')}")
