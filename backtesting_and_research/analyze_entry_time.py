import pandas as pd

df = pd.read_csv("results/robustness_sweep.csv")

# Filter for HEDGE_HIGH_RR across different entry times to answer analysis question #1
print("IMPACT OF ENTRY TIME ON EXPECTED VALUE (HEDGE_HIGH_RR, Base Scenario)")
entry_time_impact = df[
    (df["cohort"] == "HEDGE_HIGH_RR") & 
    (df["scenario"] == "Base") &
    (df["max_hold_days"] == 3) &
    (df["target_pct"] == 0.40) & 
    (df["stop_pct"] == -0.25)
].sort_values(by="entry_time")

print(entry_time_impact[["entry_time", "win_rate", "stop_rate", "timeout_rate", "expected_value", "sample_size_valid"]].to_string(index=False))

