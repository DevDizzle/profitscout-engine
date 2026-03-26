from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project="profitscout-fida8")

query = """
SELECT 
    scan_date, ticker, direction, is_skipped, skip_reason, 
    VIX_at_entry, exit_reason, realized_return_pct
FROM `profitscout-fida8.profit_scout.forward_paper_ledger`
ORDER BY scan_date DESC
"""
df = client.query(query).to_dataframe()
print(df.to_string(index=False))

