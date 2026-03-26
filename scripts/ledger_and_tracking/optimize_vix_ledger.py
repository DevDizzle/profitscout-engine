import os
from google.cloud import bigquery
import pandas as pd
import numpy as np

PROJECT_ID = "profitscout-fida8"

def optimize_ledger():
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.profit_scout.forward_paper_ledger`
    WHERE is_skipped = FALSE
      AND realized_return_pct IS NOT NULL
    """
    
    df = client.query(query).to_dataframe()
    df['realized_return_pct'] = pd.to_numeric(df['realized_return_pct'], errors='coerce')
    df['VIX_at_entry'] = pd.to_numeric(df['VIX_at_entry'], errors='coerce')
    
    base_trades = len(df)
    base_win_rate = len(df[df['realized_return_pct'] > 0]) / base_trades
    base_ev = df['realized_return_pct'].mean()
    
    print(f"Base Executed Trades: {base_trades}")
    print(f"Base Win Rate: {base_win_rate:.2%}, Base EV: {base_ev:.2%}\n")
    
    results = []
    
    vix_max_thresholds = [22, 23, 24, 25]
    pivot_values = [18, 19, 20, 21, 22]
    
    for max_vix in vix_max_thresholds:
        for pivot in pivot_values:
            for min_premium in [1, 2]:
                sim_df = df.copy()
                
                # Apply strict VIX Max
                sim_df = sim_df[sim_df["VIX_at_entry"] <= max_vix]
                
                # Apply premium score
                sim_df = sim_df[sim_df["premium_score"] >= min_premium]
                
                # Apply pivot logic
                # Skip BULLISH if VIX > pivot
                sim_df = sim_df[~((sim_df["VIX_at_entry"] >= pivot) & (sim_df["direction"] == "BULLISH"))]
                # Skip BEARISH if VIX < pivot
                sim_df = sim_df[~((sim_df["VIX_at_entry"] < pivot) & (sim_df["direction"] == "BEARISH"))]
                
                if len(sim_df) > 0:
                    ev = sim_df["realized_return_pct"].mean()
                    wr = len(sim_df[sim_df["realized_return_pct"] > 0]) / len(sim_df)
                    results.append({
                        "max_vix": max_vix,
                        "pivot": pivot,
                        "min_premium": min_premium,
                        "trades": len(sim_df),
                        "win_rate": wr,
                        "ev": ev,
                        "total_return": sim_df["realized_return_pct"].sum()
                    })
                    
    results_df = pd.DataFrame(results)
    top_ev = results_df.sort_values("ev", ascending=False).head(15)
    top_total = results_df.sort_values("total_return", ascending=False).head(15)
    
    print("--- TOP 15 BY EXPECTED VALUE (EV) ---")
    print(top_ev.to_string(index=False))
    
    print("\n--- TOP 15 BY TOTAL RETURN SUM ---")
    print(top_total.to_string(index=False))

if __name__ == "__main__":
    optimize_ledger()
