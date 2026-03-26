import requests

FMP_API_KEY = "Lzi8DIqT0fM1zV8rLQlon2NwGX8DrePT"
start_date = "2026-02-24"
end_date = "2026-03-17" 

spy_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/SPY?from={start_date}&to={end_date}&apikey={FMP_API_KEY}"
spy_res = requests.get(spy_url, timeout=10).json()
print("Direct request response:", spy_res)
