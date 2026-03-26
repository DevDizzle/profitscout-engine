import os
from google.cloud import bigquery
import pandas as pd

PROJECT_ID = "profitscout-fida8"

def print_sniper_trades():
    client = bigquery.Client(project=PROJECT_ID)
    
    # Query for ML Sniper profile trades
    query = f"""
    SELECT 
        scan_date, 
        ticker, 
        direction, 
        premium_score,
        VIX_at_entry,
        entry_price,
        target_price,
        stop_price,
        exit_reason,
        realized_return_pct
    FROM `{PROJECT_ID}.profit_scout.forward_paper_ledger`
    WHERE is_skipped = FALSE
      AND realized_return_pct IS NOT NULL
      AND VIX_at_entry <= 23.0
      AND premium_score >= 2
      AND NOT (VIX_at_entry >= 18.0 AND direction = 'BULLISH')
      AND NOT (VIX_at_entry < 18.0 AND direction = 'BEARISH')
    ORDER BY scan_date ASC
    """
    
    try:
        df = client.query(query).to_dataframe()
    except Exception as e:
        print(f"Error querying BigQuery: {e}")
        return

    print("\n--- ML SNIPER TRADES (HISTORICAL COHORT) ---")
    
    if len(df) == 0:
        print("No trades found in this cohort.")
        return
        
    df['realized_return_pct'] = pd.to_numeric(df['realized_return_pct'], errors='coerce')
    
    # Format for clean printing
    df['scan_date'] = pd.to_datetime(df['scan_date']).dt.strftime('%Y-%m-%d')
    df['VIX_at_entry'] = df['VIX_at_entry'].round(2)
    df['entry_price'] = df['entry_price'].map('${:.2f}'.format)
    df['realized_return_pct'] = (df['realized_return_pct'] * 100).round(2).astype(str) + '%'
    
    # Print as a nice table
    print(df.to_string(index=False))
    
    print("\nSummary:")
    print(f"Total Trades: {len(df)}")
    wins = len(df[df['realized_return_pct'].str.replace('%', '').astype(float) > 0])
    print(f"Wins: {wins}")
    print(f"Losses: {len(df) - wins}")
    print(f"Win Rate: {(wins/len(df))*100:.1f}%")

if __name__ == "__main__":
    print_sniper_trades()
