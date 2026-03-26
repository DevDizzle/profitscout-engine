import pandas as pd
from google.cloud import bigquery

client = bigquery.Client(project="profitscout-fida8")

query = """
SELECT column_name, data_type 
FROM `profitscout-fida8.profit_scout.INFORMATION_SCHEMA.COLUMNS` 
WHERE table_name = "overnight_signals"
"""
schema_df = client.query(query).to_dataframe()

liquidity_cols = [
    "recommended_volume", "recommended_oi", "recommended_spread_pct", 
    "recommended_mid_price", "day_volume", "total_options_dollar_volume",
    "call_active_strikes", "put_active_strikes"
]

print("=== UPSTREAM LIQUIDITY FIELDS PRESENCE ===")
for col in liquidity_cols:
    exists = col in schema_df["column_name"].values
    dtype = schema_df[schema_df["column_name"] == col]["data_type"].values[0] if exists else "N/A"
    print(f"{col}: {exists} ({dtype})")

query_nulls = """
SELECT 
    COUNT(*) as total_rows,
    COUNTIF(recommended_volume IS NOT NULL) as has_vol,
    COUNTIF(recommended_oi IS NOT NULL) as has_oi,
    COUNTIF(recommended_spread_pct IS NOT NULL) as has_spread,
    COUNTIF(recommended_mid_price IS NOT NULL) as has_mid
FROM `profitscout-fida8.profit_scout.overnight_signals`
"""
nulls_df = client.query(query_nulls).to_dataframe()
print("\n=== POPULATION RELIABILITY ===")
print(nulls_df.to_string(index=False))


