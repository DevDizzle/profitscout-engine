import os
from google.cloud import bigquery
import pandas as pd
import numpy as np

PROJECT_ID = "profitscout-fida8"

def run_eda():
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.profit_scout.forward_paper_ledger`
    """
    
    try:
        df = client.query(query).to_dataframe()
    except Exception as e:
        print(f"Error querying BigQuery: {e}")
        return

    print("=== EXTENSIVE EDA: FORWARD PAPER LEDGER ===")
    print(f"Total records found: {len(df)}")
    
    if len(df) == 0:
        print("No data available to analyze.")
        return

    # 1. Basic Counts
    skipped_count = df['is_skipped'].sum()
    executed_df = df[df['is_skipped'] == False].copy()
    executed_count = len(executed_df)
    
    print(f"\nTotal Executed Trades: {executed_count}")
    print(f"Total Skipped Signals: {skipped_count}")
    
    if skipped_count > 0:
        print("\nSkip Reasons Distribution:")
        print(df['skip_reason'].value_counts())
        
    if executed_count == 0:
        print("No executed trades to analyze performance.")
        return

    # Clean numerical columns
    executed_df['realized_return_pct'] = pd.to_numeric(executed_df['realized_return_pct'], errors='coerce')
    executed_df = executed_df.dropna(subset=['realized_return_pct'])
    
    executed_count_clean = len(executed_df)
    print(f"\nTotal Executed Trades with valid returns: {executed_count_clean}")
    
    if executed_count_clean == 0:
        print("No valid return data to analyze.")
        return

    # 2. Overall Performance
    wins = executed_df[executed_df['realized_return_pct'] > 0]
    win_rate = len(wins) / executed_count_clean
    avg_return = executed_df['realized_return_pct'].mean()
    median_return = executed_df['realized_return_pct'].median()
    
    print(f"\n--- OVERALL PERFORMANCE ---")
    print(f"Win Rate: {win_rate:.2%}")
    print(f"Average Return: {avg_return:.2%}")
    print(f"Median Return: {median_return:.2%}")
    print(f"Exit Reasons:")
    print(executed_df['exit_reason'].value_counts())
    
    # 3. Performance by Direction
    print(f"\n--- PERFORMANCE BY DIRECTION ---")
    for direction in executed_df['direction'].unique():
        dir_df = executed_df[executed_df['direction'] == direction]
        if len(dir_df) > 0:
            dir_win_rate = len(dir_df[dir_df['realized_return_pct'] > 0]) / len(dir_df)
            dir_avg_return = dir_df['realized_return_pct'].mean()
            print(f"{direction} (n={len(dir_df)}):")
            print(f"  Win Rate: {dir_win_rate:.2%}")
            print(f"  Avg Return: {dir_avg_return:.2%}")

    # 4. Performance by Premium Score
    print(f"\n--- PERFORMANCE BY PREMIUM SCORE ---")
    for score in sorted(executed_df['premium_score'].unique()):
        score_df = executed_df[executed_df['premium_score'] == score]
        if len(score_df) > 0:
            score_win_rate = len(score_df[score_df['realized_return_pct'] > 0]) / len(score_df)
            score_avg_return = score_df['realized_return_pct'].mean()
            print(f"Score {score} (n={len(score_df)}):")
            print(f"  Win Rate: {score_win_rate:.2%}")
            print(f"  Avg Return: {score_avg_return:.2%}")

    # 5. Performance by SPY Trend State
    print(f"\n--- PERFORMANCE BY SPY TREND STATE ---")
    for state in executed_df['SPY_trend_state'].unique():
        state_df = executed_df[executed_df['SPY_trend_state'] == state]
        if len(state_df) > 0:
            state_win_rate = len(state_df[state_df['realized_return_pct'] > 0]) / len(state_df)
            state_avg_return = state_df['realized_return_pct'].mean()
            print(f"{state} (n={len(state_df)}):")
            print(f"  Win Rate: {state_win_rate:.2%}")
            print(f"  Avg Return: {state_avg_return:.2%}")
            
    # 6. Performance by VIX Level Buckets
    print(f"\n--- PERFORMANCE BY VIX LEVEL BUCKETS ---")
    vix_bins = [0, 15, 18, 20, 25, 100]
    vix_labels = ['<15', '15-18', '18-20', '20-25', '>25']
    executed_df['vix_bucket'] = pd.cut(pd.to_numeric(executed_df['VIX_at_entry'], errors='coerce'), bins=vix_bins, labels=vix_labels)
    
    for bucket in vix_labels:
        bucket_df = executed_df[executed_df['vix_bucket'] == bucket]
        if len(bucket_df) > 0:
            bucket_win_rate = len(bucket_df[bucket_df['realized_return_pct'] > 0]) / len(bucket_df)
            bucket_avg_return = bucket_df['realized_return_pct'].mean()
            print(f"VIX {bucket} (n={len(bucket_df)}):")
            print(f"  Win Rate: {bucket_win_rate:.2%}")
            print(f"  Avg Return: {bucket_avg_return:.2%}")

if __name__ == "__main__":
    run_eda()
