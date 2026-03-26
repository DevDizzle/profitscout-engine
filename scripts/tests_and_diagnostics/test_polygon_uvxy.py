import os
import requests

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")

url = f"https://api.polygon.io/v2/aggs/ticker/UVXY/range/1/day/2026-03-01/2026-03-16?adjusted=true&sort=asc&apiKey={POLYGON_API_KEY}"
res = requests.get(url).json()
print("UVXY API Response:", res)
