import pandas as pd

df = pd.read_csv("results/robustness_sweep.csv")

# Filter for valid trades and a minimum sample size
# and sort to find the best configurations
top_configs = df[
    (df["sample_size_valid"] >= 10) & 
    (df["scenario"] == "Base")
].sort_values(by="expected_value", ascending=False).head(20)

print("TOP BASE SCENARIOS BY EXPECTED VALUE")
print(top_configs[["cohort", "entry_time", "target_pct", "stop_pct", "max_hold_days", "win_rate", "stop_rate", "expected_value", "sample_size_valid"]].to_string(index=False))

print("\nWORST SCENARIOS BY EXPECTED VALUE")
worst_configs = df[
    (df["sample_size_valid"] >= 10) & 
    (df["scenario"] == "Base")
].sort_values(by="expected_value", ascending=True).head(10)
print(worst_configs[["cohort", "entry_time", "target_pct", "stop_pct", "max_hold_days", "win_rate", "stop_rate", "expected_value", "sample_size_valid"]].to_string(index=False))

