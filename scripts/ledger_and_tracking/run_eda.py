import pandas as pd
from google.cloud import bigquery
import os

PROJECT_ID = "profitscout-fida8"
DATASET_ID = "profit_scout"
TABLE_ID = "forward_paper_ledger"

client = bigquery.Client(project=PROJECT_ID)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

query = f"""
    SELECT *
    FROM `{table_ref}`
"""

print("Fetching data from BigQuery...")
df = client.query(query).to_dataframe()

print("\n--- Exploratory Data Analysis: Forward Paper Ledger ---")
print(f"Total Signals Processed: {len(df)}")

if len(df) == 0:
    print("No data found in the table.")
    exit()

print(f"Date Range: {df['scan_date'].min()} to {df['scan_date'].max()}")

print("\n1. Execution Overview:")
execution_counts = df['is_skipped'].value_counts(dropna=False)
executed = execution_counts.get(False, 0)
skipped = execution_counts.get(True, 0)
print(f"Executed Trades: {executed} ({executed/len(df):.1%})")
print(f"Skipped Trades: {skipped} ({skipped/len(df):.1%})")

print("\n2. Skip Reasons Distribution:")
if skipped > 0:
    skip_reasons = df[df['is_skipped'] == True]['skip_reason'].value_counts()
    for reason, count in skip_reasons.items():
        print(f"  - {reason}: {count} ({count/skipped:.1%})")
else:
    print("  None")

print("\n3. Premium Signal Breakdown (All Signals):")
premium_counts = df.groupby(['is_premium_signal', 'premium_score']).size().reset_index(name='count')
for _, row in premium_counts.iterrows():
    print(f"  - Premium: {row['is_premium_signal']}, Score: {row['premium_score']} -> Count: {row['count']}")

# Analyze Executed Trades
df_exec = df[df['is_skipped'] == False].copy()

if len(df_exec) > 0:
    print(f"\n4. Executed Trades Analysis (N={len(df_exec)}):")
    
    # Calculate Win Rate
    # Assuming exit_reason 'TARGET' means a win, 'STOP' means a loss. If realized_return_pct is available, use that.
    # Let's check both.
    wins = df_exec[df_exec['realized_return_pct'] > 0]
    losses = df_exec[df_exec['realized_return_pct'] < 0]
    flat = df_exec[df_exec['realized_return_pct'] == 0]
    unclosed = df_exec[df_exec['realized_return_pct'].isna()]
    
    closed_trades = len(wins) + len(losses) + len(flat)
    
    print(f"  Closed Trades: {closed_trades}")
    print(f"  Open/Unclosed Trades: {len(unclosed)}")
    
    if closed_trades > 0:
        win_rate = len(wins) / closed_trades
        avg_return = df_exec['realized_return_pct'].mean()
        print(f"  Win Rate (Closed Trades): {win_rate:.1%} ({len(wins)}W / {len(losses)}L / {len(flat)}Flat)")
        print(f"  Average Realized Return: {avg_return:.2%}")
        
        print("\n5. Performance by Premium Score (Closed Trades Only):")
        closed_df = df_exec.dropna(subset=['realized_return_pct'])
        score_perf = closed_df.groupby('premium_score').agg(
            Count=('ticker', 'size'),
            Win_Rate=('realized_return_pct', lambda x: (x > 0).mean()),
            Avg_Return=('realized_return_pct', 'mean')
        ).reset_index()
        for _, row in score_perf.iterrows():
            print(f"  - Score {row['premium_score']}: {row['Count']} trades | Win Rate: {row['Win_Rate']:.1%} | Avg Return: {row['Avg_Return']:.2%}")

        print("\n6. Performance by Direction (Closed Trades Only):")
        dir_perf = closed_df.groupby('direction').agg(
            Count=('ticker', 'size'),
            Win_Rate=('realized_return_pct', lambda x: (x > 0).mean()),
            Avg_Return=('realized_return_pct', 'mean')
        ).reset_index()
        for _, row in dir_perf.iterrows():
            print(f"  - {row['direction']}: {row['Count']} trades | Win Rate: {row['Win_Rate']:.1%} | Avg Return: {row['Avg_Return']:.2%}")
            
        print("\n7. Performance by SPY Trend State:")
        regime_perf = closed_df.groupby('SPY_trend_state').agg(
            Count=('ticker', 'size'),
            Win_Rate=('realized_return_pct', lambda x: (x > 0).mean()),
            Avg_Return=('realized_return_pct', 'mean')
        ).reset_index()
        for _, row in regime_perf.iterrows():
            print(f"  - {row['SPY_trend_state']}: {row['Count']} trades | Win Rate: {row['Win_Rate']:.1%} | Avg Return: {row['Avg_Return']:.2%}")

else:
    print("\nNo executed trades found to analyze performance.")

print("\n--- End of EDA ---")
