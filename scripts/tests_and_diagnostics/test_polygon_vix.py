import requests

url = "https://api.polygon.io/v2/snapshot/locale/us/markets/indices/tickers/I:VIX?apiKey="
res = requests.get(url)
print("VIX Snapshot Text:", res.text)

