import pandas as pd

df = pd.read_csv("results/robustness_sweep.csv")

# Filter for Cohort Comparison (15:00 Entry, 3-Day Hold, +40%/-25%, Base Scenario)
print("COHORT COMPARISON (15:00 Entry, 3-Day Hold, +40%/-25%, Base Scenario)")
cohort_impact = df[
    (df["entry_time"] == "15:00") & 
    (df["scenario"] == "Base") &
    (df["max_hold_days"] == 3) &
    (df["target_pct"] == 0.40) & 
    (df["stop_pct"] == -0.25)
].sort_values(by="expected_value", ascending=False)

print(cohort_impact[["cohort", "win_rate", "stop_rate", "timeout_rate", "expected_value", "sample_size_valid"]].to_string(index=False))

