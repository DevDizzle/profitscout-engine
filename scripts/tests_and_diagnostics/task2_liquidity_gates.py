import pandas as pd
import numpy as np
from google.cloud import bigquery

client = bigquery.Client(project="profitscout-fida8")

query = """
SELECT 
    ticker, scan_date, direction, recommended_contract, recommended_strike, 
    recommended_expiration, recommended_volume, recommended_oi, recommended_spread_pct,
    recommended_mid_price, premium_hedge, premium_high_rr, premium_high_atr, 
    is_premium_signal, premium_score, is_win, peak_return_3d
FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
WHERE recommended_strike IS NOT NULL 
  AND recommended_expiration IS NOT NULL
  AND performance_updated IS NOT NULL
"""
df = client.query(query).to_dataframe()

df["HEDGE_HIGH_RR"] = df["premium_hedge"] & df["premium_high_rr"]
df["HEDGE_HIGH_ATR"] = df["premium_hedge"] & df["premium_high_atr"]
df["SCORE_GTE_2"] = df["premium_score"] >= 2
df["SCORE_GTE_3"] = df["premium_score"] >= 3

cohorts = ["HEDGE_HIGH_RR", "HEDGE_HIGH_ATR", "SCORE_GTE_2", "SCORE_GTE_3"]

gates = {
    "None": lambda df: df,
    "V>25 | OI>50": lambda df: df[(df["recommended_volume"] >= 25) | (df["recommended_oi"] >= 50)],
    "V>50 | OI>100": lambda df: df[(df["recommended_volume"] >= 50) | (df["recommended_oi"] >= 100)],
    "V>100 | OI>250": lambda df: df[(df["recommended_volume"] >= 100) | (df["recommended_oi"] >= 250)],
    "Spread<=15%": lambda df: df[df["recommended_spread_pct"] <= 0.15],
    "Spread<=10%": lambda df: df[df["recommended_spread_pct"] <= 0.10],
    "V>50|OI>100 & Spread<=15%": lambda df: df[((df["recommended_volume"] >= 50) | (df["recommended_oi"] >= 100)) & (df["recommended_spread_pct"] <= 0.15)],
    "V>50|OI>100 & Spread<=15% & Mid>=0.50": lambda df: df[((df["recommended_volume"] >= 50) | (df["recommended_oi"] >= 100)) & (df["recommended_spread_pct"] <= 0.15) & (df["recommended_mid_price"] >= 0.50)],
}

results = []

for cohort in cohorts:
    cohort_df = df[df[cohort] == True]
    total_cohort = len(cohort_df)
    if total_cohort == 0: continue
    
    for gate_name, gate_func in gates.items():
        gated_df = gate_func(cohort_df)
        pass_count = len(gated_df)
        if pass_count == 0: continue
        
        days_span = (pd.to_datetime(gated_df["scan_date"].max()) - pd.to_datetime(gated_df["scan_date"].min())).days
        months_span = max(1, days_span / 30.0)
        monthly_trades = pass_count / months_span
        
        results.append({
            "Gate": gate_name,
            "Cohort": cohort,
            "Pass Count": pass_count,
            "Pass Rate": f"{(pass_count/total_cohort)*100:.1f}%",
            "Monthly Trades": f"{monthly_trades:.1f}",
            "Avg Spread": f"{gated_df['recommended_spread_pct'].mean():.3f}",
            "Avg Volume": f"{gated_df['recommended_volume'].mean():.0f}",
            "Avg OI": f"{gated_df['recommended_oi'].mean():.0f}",
            "Directional Win Rate": f"{(gated_df['is_win'].sum() / pass_count)*100:.1f}%",
            "Avg Peak Return 3D": round(gated_df['peak_return_3d'].mean(), 2)
        })

res_df = pd.DataFrame(results).sort_values(by=["Cohort", "Avg Peak Return 3D"], ascending=[True, False])
print(res_df.to_string(index=False))

res_df.to_csv("liquidity_gates_analysis.csv", index=False)
