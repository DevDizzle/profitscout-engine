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
print("VIX REGIME HYPOTHESIS TEST: Bulls vs Bears")
print("="*80)

# Let"s test a few VIX pivot points (e.g., 18, 20, 22)
pivots = [18.0, 20.0, 22.0, 24.0]

for pivot in pivots:
    print(f"\n--- Testing VIX Pivot at {pivot} ---")
    
    # LOW VIX Regime -> Bulls
    low_vix_bulls = valid_df[(valid_df["vix_level"] < pivot) & (valid_df["direction"] == "BULLISH")]
    if len(low_vix_bulls) > 0:
        ev = low_vix_bulls["wrapper_return"].mean()
        wr = len(low_vix_bulls[low_vix_bulls["exit_reason"] == "target"]) / len(low_vix_bulls)
        print(f"  LOW VIX (<{pivot}) BULLS: {len(low_vix_bulls)} trades | EV: {ev*100:.2f}% | Win: {wr*100:.1f}%")
    else:
        print(f"  LOW VIX (<{pivot}) BULLS: 0 trades")
        
    # HIGH VIX Regime -> Bears
    high_vix_bears = valid_df[(valid_df["vix_level"] >= pivot) & (valid_df["direction"] == "BEARISH")]
    if len(high_vix_bears) > 0:
        ev = high_vix_bears["wrapper_return"].mean()
        wr = len(high_vix_bears[high_vix_bears["exit_reason"] == "target"]) / len(high_vix_bears)
        print(f"  HIGH VIX (>={pivot}) BEARS: {len(high_vix_bears)} trades | EV: {ev*100:.2f}% | Win: {wr*100:.1f}%")
    else:
        print(f"  HIGH VIX (>={pivot}) BEARS: 0 trades")


