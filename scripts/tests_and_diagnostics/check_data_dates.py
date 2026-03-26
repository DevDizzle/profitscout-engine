from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project="profitscout-fida8")

# Let"s check the earliest dates available in the overnight_signals table
query = """
SELECT 
    MIN(scan_date) as earliest_date,
    MAX(scan_date) as latest_date,
    COUNT(*) as total_signals
FROM `profitscout-fida8.profit_scout.overnight_signals`
"""
df = client.query(query).to_dataframe()
print(df.to_string(index=False))

# Now let"s check how many of those have the recommended contract fields populated
query2 = """
SELECT 
    MIN(scan_date) as earliest_date,
    MAX(scan_date) as latest_date,
    COUNT(*) as signals_with_contracts
FROM `profitscout-fida8.profit_scout.overnight_signals`
WHERE recommended_strike IS NOT NULL 
  AND recommended_expiration IS NOT NULL
"""
df2 = client.query(query2).to_dataframe()
print("\nSignals with Options Contracts:")
print(df2.to_string(index=False))

