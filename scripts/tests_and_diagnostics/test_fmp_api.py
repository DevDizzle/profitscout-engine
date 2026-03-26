import os
import requests

FMP_API_KEY = os.environ.get("FMP_API_KEY", "")
start_date = "2026-02-24"
end_date = "2026-03-17" 

# Test if it returns 404 with missing API key?
# Because the cloud run log just says "Missing historical data from FMP API" and doesn't print the API error.
spy_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/SPY?from={start_date}&to={end_date}&apikey="
spy_res = requests.get(spy_url, timeout=10).json()
print("Without API Key:", spy_res)
