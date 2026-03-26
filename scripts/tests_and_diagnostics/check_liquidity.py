import pandas as pd
from google.cloud import bigquery

client = bigquery.Client(project="profitscout-fida8")

query = """
SELECT 
    ticker, scan_date, recommended_volume, recommended_oi, recommended_spread_pct, day_volume, total_options_dollar_volume
FROM `profitscout-fida8.profit_scout.overnight_signals`
WHERE overnight_score >= 6
ORDER BY scan_date DESC
LIMIT 10
"""
df = client.query(query).to_dataframe()
print(df.to_string())

