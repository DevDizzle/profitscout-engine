import pandas as pd
import numpy as np

# Load the historical dataset
df = pd.read_csv("backtesting_and_research/deep_pattern_analysis.csv")

# Clean VIX level and change columns which got written as strings instead of floats
def extract_float(val):
    if isinstance(val, str):
        try:
            return float(val.split()[-1])
        except:
            return np.nan
    return float(val)

df["vix_level"] = df["vix_level"].apply(extract_float)

# Filter out invalid liquidity trades to focus on execution outcomes
valid_df = df[df["exit_reason"] != "invalid_liquidity"].copy()

print(f"Total Valid Executable Trades in Historical Backtest: {len(valid_df)}")

# Base metrics
base_ev = valid_df["wrapper_return"].mean()
base_win_rate = len(valid_df[valid_df["exit_reason"] == "target"]) / len(valid_df)
print(f"Base Win Rate: {base_win_rate*100:.2f}%, Base EV: {base_ev*100:.2f}%\n")

# Let's grid search VIX thresholds and direction rules
vix_max_thresholds = [20, 22, 25, 30, 100]
vix_chop_zones = [(18, 20), (16, 20), (18, 22), (15, 20), None]

results = []

for max_vix in vix_max_thresholds:
    for chop_zone in vix_chop_zones:
        # Create a copy
        sim_df = valid_df.copy()
        
        # Apply strict VIX Max
        sim_df = sim_df[sim_df["vix_level"] <= max_vix]
        
        # Apply chop zone skip rule
        if chop_zone is not None:
            chop_min, chop_max = chop_zone
            # Rule: skip all trades in chop zone, or maybe skip counter-trend?
            # Let's test two variations:
            
            # Variant A: Skip ALL trades in chop zone
            sim_df_a = sim_df[~((sim_df["vix_level"] >= chop_min) & (sim_df["vix_level"] <= chop_max))]
            
            if len(sim_df_a) > 0:
                ev_a = sim_df_a["wrapper_return"].mean()
                wr_a = len(sim_df_a[sim_df_a["exit_reason"] == "target"]) / len(sim_df_a)
                results.append({
                    "max_vix": max_vix,
                    "chop_zone": f"{chop_min}-{chop_max}",
                    "chop_rule": "Skip All in Zone",
                    "trades": len(sim_df_a),
                    "win_rate": wr_a,
                    "ev": ev_a
                })
                
            # Variant B: Skip BEARISH trades when VIX is low, skip BULLISH trades when VIX is high
            # VIX Pivot Logic: if VIX > pivot, skip Bullish. If VIX < pivot, skip Bearish.
            # Let's parameterize the pivot
            
pivot_values = [18, 19, 20, 21, 22]
for max_vix in [22, 25, 30]:
    for pivot in pivot_values:
        for min_premium in [1, 2]:
            sim_df = valid_df.copy()
            sim_df = sim_df[sim_df["vix_level"] <= max_vix]
            sim_df = sim_df[sim_df["premium_score"] >= min_premium]
            
            # Apply pivot logic
            # Skip BULLISH if VIX > pivot
            sim_df = sim_df[~((sim_df["vix_level"] >= pivot) & (sim_df["direction"] == "BULLISH"))]
            # Skip BEARISH if VIX < pivot
            sim_df = sim_df[~((sim_df["vix_level"] < pivot) & (sim_df["direction"] == "BEARISH"))]
            
            if len(sim_df) > 0:
                ev = sim_df["wrapper_return"].mean()
                wr = len(sim_df[sim_df["exit_reason"] == "target"]) / len(sim_df)
                results.append({
                    "max_vix": max_vix,
                    "chop_zone": f"Pivot at {pivot}",
                    "chop_rule": f"Pivot Logic (Min Prem {min_premium})",
                    "trades": len(sim_df),
                    "win_rate": wr,
                    "ev": ev
                })
            

# Convert to DataFrame and sort by EV
results_df = pd.DataFrame(results)
top_ev = results_df.sort_values("ev", ascending=False).head(15)
top_wr = results_df.sort_values("win_rate", ascending=False).head(15)

print("--- TOP 15 BY EXPECTED VALUE (EV) ---")
print(top_ev.to_string(index=False))

print("\n--- TOP 15 BY WIN RATE ---")
print(top_wr.to_string(index=False))

