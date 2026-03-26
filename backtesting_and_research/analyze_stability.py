import pandas as pd

df = pd.read_csv("results/robustness_sweep.csv")

# Filter for the most promising strategy (HEDGE_HIGH_RR at 15:00)
# and show performance across scenarios
print("STABILITY CHECK: HEDGE_HIGH_RR @ 15:00, 3 Day Hold, +40% / -25%")
stable_check = df[
    (df["cohort"] == "HEDGE_HIGH_RR") & 
    (df["entry_time"] == "15:00") & 
    (df["target_pct"] == 0.40) & 
    (df["stop_pct"] == -0.25) & 
    (df["max_hold_days"] == 3)
]
print(stable_check[["scenario", "win_rate", "stop_rate", "timeout_rate", "expected_value", "median_return", "sample_size_valid"]].to_string(index=False))

print("\nSTABILITY CHECK: HEDGE_HIGH_RR @ 15:00, 3 Day Hold, +50% / -40%")
stable_check2 = df[
    (df["cohort"] == "HEDGE_HIGH_RR") & 
    (df["entry_time"] == "15:00") & 
    (df["target_pct"] == 0.50) & 
    (df["stop_pct"] == -0.40) & 
    (df["max_hold_days"] == 3)
]
print(stable_check2[["scenario", "win_rate", "stop_rate", "timeout_rate", "expected_value", "median_return", "sample_size_valid"]].to_string(index=False))


