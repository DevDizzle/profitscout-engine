import pandas as pd
from google.cloud import bigquery

PROJECT_ID = "profitscout-fida8"

def check_vix():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
    SELECT MIN(VIX_at_entry) as min_vix, MAX(VIX_at_entry) as max_vix
    FROM `{PROJECT_ID}.profit_scout.forward_paper_ledger`
    WHERE is_skipped = FALSE
    """
    df = client.query(query).to_dataframe()
    print(df)
    
    query2 = f"""
    SELECT VIX_at_entry, direction, premium_score, COUNT(*) as cnt
    FROM `{PROJECT_ID}.profit_scout.forward_paper_ledger`
    WHERE is_skipped = FALSE AND VIX_at_entry < 18
    GROUP BY 1, 2, 3
    """
    df2 = client.query(query2).to_dataframe()
    print("\nTrades when VIX < 18:")
    print(df2)

if __name__ == "__main__":
    check_vix()
