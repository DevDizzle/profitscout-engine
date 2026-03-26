import pandas as pd
import numpy as np

# Load the dataset
df = pd.read_csv("deep_pattern_analysis.csv")

# Clean VIX level and change columns which got written as strings instead of floats
def extract_float(val):
    if isinstance(val, str):
        try:
            return float(val.split()[-1])
        except:
            return np.nan
    return float(val)

df["vix_level"] = df["vix_level"].apply(extract_float)
df["vix_change"] = df["vix_change"].apply(extract_float)

# Filter out invalid liquidity trades to focus on execution outcomes
valid_df = df[df["exit_reason"] != "invalid_liquidity"].copy()

# Phase 3: Cluster Analysis on Losers
print("="*80)
print("PHASE 3: IDENTIFYING SKIP CONDITIONS")
print(f"Total Valid Executable Trades: {len(valid_df)}")
print("="*80)

# Create outcome buckets
valid_df["is_loss"] = valid_df["exit_reason"] == "stop"
losers = valid_df[valid_df["is_loss"]]
winners = valid_df[valid_df["exit_reason"] == "target"]
timeouts = valid_df[valid_df["exit_reason"] == "timeout"]

print(f"Outcome Distribution:")
print(f"  Target (+40%): {len(winners)} ({len(winners)/len(valid_df)*100:.1f}%)")
print(f"  Stop (-25%): {len(losers)} ({len(losers)/len(valid_df)*100:.1f}%)")
print(f"  Timeout: {len(timeouts)} ({len(timeouts)/len(valid_df)*100:.1f}%)\n")

# Analyzing clusters in the losing trades compared to the whole set
features_to_check = [
    "vix_level", "vix_change", "rec_iv", "rec_spread", "rec_dte", 
    "premium_score", "atr_move"
]

for feature in features_to_check:
    print(f"--- Analyzing {feature} ---")
    print(f"Overall Median: {valid_df[feature].median():.3f} | Losers Median: {losers[feature].median():.3f} | Winners Median: {winners[feature].median():.3f}")
    
    # Calculate stop rate at different thresholds
    q75 = valid_df[feature].quantile(0.75)
    q25 = valid_df[feature].quantile(0.25)
    
    # High cluster
    high_subset = valid_df[valid_df[feature] >= q75]
    if len(high_subset) > 0:
        high_stop_rate = len(high_subset[high_subset["is_loss"]]) / len(high_subset)
        print(f"  Top 25% (>={q75:.3f}) Stop Rate: {high_stop_rate*100:.1f}% (N={len(high_subset)})")
        
    # Low cluster
    low_subset = valid_df[valid_df[feature] <= q25]
    if len(low_subset) > 0:
        low_stop_rate = len(low_subset[low_subset["is_loss"]]) / len(low_subset)
        print(f"  Bottom 25% (<={q25:.3f}) Stop Rate: {low_stop_rate*100:.1f}% (N={len(low_subset)})\n")

print("--- Analyzing Qualitative / Categorical Features ---")
# Flow Intent
print("\nStop Rate by Flow Intent:")
for intent in valid_df["flow_intent"].unique():
    subset = valid_df[valid_df["flow_intent"] == intent]
    if len(subset) > 0:
        stop_rate = len(subset[subset["is_loss"]]) / len(subset)
        print(f"  {intent}: {stop_rate*100:.1f}% (N={len(subset)})")

# SPY Trend
print("\nStop Rate by SPY Trend:")
for trend in valid_df["spy_trend"].unique():
    subset = valid_df[valid_df["spy_trend"] == trend]
    if len(subset) > 0:
        stop_rate = len(subset[subset["is_loss"]]) / len(subset)
        print(f"  {trend}: {stop_rate*100:.1f}% (N={len(subset)})")

print("\n--- Identifying Potential Skip Gates ---")
# Example hard gates
gates = [
    ("High VIX (> 25)", valid_df["vix_level"] > 25.0),
    ("Short DTE (< 14 days)", valid_df["rec_dte"] < 14),
    ("Wide Spread (> 20%)", valid_df["rec_spread"] > 0.20),
    ("Bearish SPY Trend", valid_df["spy_trend"] == "BEARISH")
]

base_ev = valid_df["wrapper_return"].mean()

for gate_name, condition in gates:
    filtered_df = valid_df[~condition] # keep trades where condition is FALSE
    if len(filtered_df) == 0: continue
    
    new_ev = filtered_df["wrapper_return"].mean()
    new_win_rate = len(filtered_df[filtered_df["exit_reason"] == "target"]) / len(filtered_df)
    new_stop_rate = len(filtered_df[filtered_df["exit_reason"] == "stop"]) / len(filtered_df)
    
    print(f"Gate: Skip if {gate_name}")
    print(f"  Trades remaining: {len(filtered_df)} / {len(valid_df)}")
    print(f"  New EV: {new_ev*100:.2f}% (vs Base {base_ev*100:.2f}%)")
    print(f"  New Stop Rate: {new_stop_rate*100:.1f}%")
    print(f"  New Win Rate: {new_win_rate*100:.1f}%\n")

