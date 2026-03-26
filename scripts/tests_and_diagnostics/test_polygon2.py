import os
import requests
from datetime import datetime, timedelta

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")

def get_vix(date_str):
    url = f"https://api.polygon.io/v2/aggs/ticker/I:VIX/range/1/day/2026-03-01/{date_str}?adjusted=true&sort=asc&apiKey={POLYGON_API_KEY}"
    res = requests.get(url).json()
    print("VIX API Response:", res)

get_vix('2026-03-16')
