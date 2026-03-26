import pandas as pd
from datetime import date, timedelta
from google.cloud import bigquery

# Test BQ extraction
PROJECT_ID = "profitscout-fida8"
client = bigquery.Client(project=PROJECT_ID)

query = """
SELECT ticker, scan_date, recommended_contract, recommended_strike, recommended_expiration, direction, premium_hedge, premium_high_rr, premium_high_atr
FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
WHERE is_tradeable = TRUE
"""
df = client.query(query).to_dataframe()
print(f"Total tradeable signals found: {len(df)}")
print(df.head())

