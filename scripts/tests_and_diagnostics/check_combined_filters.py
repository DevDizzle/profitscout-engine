import pandas as pd
import numpy as np

# Load the dataset
df = pd.read_csv("deep_pattern_analysis.csv")

def extract_float(val):
    if isinstance(val, str):
        try:
            return float(val.split()[-1])
        except:
            return np.nan
    return float(val)

df["vix_level"] = df["vix_level"].apply(extract_float)

valid_df = df[df["exit_reason"] != "invalid_liquidity"].copy()
valid_df["is_loss"] = valid_df["exit_reason"] == "stop"

print("="*80)
print("COMBINED REGIME FILTER TEST (RELAXED 7)")
print("="*80)

cond_vix = valid_df["vix_level"] <= 25.0
cond_dte = valid_df["rec_dte"] >= 14
cond_direction = valid_df["direction"] == "BEARISH" # Try just BEARISH signals
cond_premium = valid_df["premium_score"] >= 2 # Added Premium Score

filtered = valid_df[cond_vix & cond_dte & cond_direction & cond_premium]

if len(filtered) > 0:
    new_ev = filtered["wrapper_return"].mean()
    new_win = len(filtered[filtered["exit_reason"] == "target"]) / len(filtered)
    new_stop = len(filtered[filtered["exit_reason"] == "stop"]) / len(filtered)
    print(f"\nFiltered Strategy (VIX<=25, DTE>=14, BEARISH, Score>=2):")
    print(f"Remaining Trades: {len(filtered)}")
    print(f"Win Rate: {new_win*100:.1f}%")
    print(f"Stop Rate: {new_stop*100:.1f}%")
    print(f"EV per trade: {new_ev*100:.2f}%")
    
    # Chronological Split on Filtered Set
    filtered = filtered.sort_values("scan_date")
    midpoint = len(filtered) // 2
    h1 = filtered.iloc[:midpoint]
    h2 = filtered.iloc[midpoint:]
    
    ev_h1 = h1["wrapper_return"].mean()
    wr_h1 = len(h1[h1["exit_reason"]=="target"])/len(h1)
    ev_h2 = h2["wrapper_return"].mean()
    wr_h2 = len(h2[h2["exit_reason"]=="target"])/len(h2)
    
    print("\nFiltered Holdout Performance:")
    print(f"First Half ({len(h1)} trades): EV = {ev_h1*100:.2f}% | Win Rate = {wr_h1*100:.1f}%")
    print(f"Second Half ({len(h2)} trades): EV = {ev_h2*100:.2f}% | Win Rate = {wr_h2*100:.1f}%")
else:
    print("No trades left.")

