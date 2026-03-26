import pandas as pd
from google.cloud import bigquery

client = bigquery.Client(project="profitscout-fida8")

query = """
SELECT 
    COUNT(*) as total_signals,
    COUNTIF(recommended_volume > 50) as high_vol,
    COUNTIF(recommended_oi > 100) as high_oi,
    COUNTIF(recommended_volume > 50 OR recommended_oi > 100) as tradable_liquidity,
    COUNTIF(recommended_spread_pct < 0.15) as tight_spread
FROM `profitscout-fida8.profit_scout.overnight_signals`
WHERE overnight_score >= 6
"""
df = client.query(query).to_dataframe()
print(df.to_string(index=False))

