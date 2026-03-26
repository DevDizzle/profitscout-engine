import pandas as pd

df = pd.read_csv("results/robustness_sweep.csv")

# Filter for HEDGE_HIGH_RR at 15:00, 3 Day hold, Base Scenario to evaluate Target/Stop
print("IMPACT OF TARGET/STOP SELECTION (HEDGE_HIGH_RR, 15:00 Entry, 3-Day Hold, Base Scenario)")
target_stop_impact = df[
    (df["cohort"] == "HEDGE_HIGH_RR") & 
    (df["entry_time"] == "15:00") & 
    (df["scenario"] == "Base") &
    (df["max_hold_days"] == 3)
].sort_values(by="expected_value", ascending=False)

print(target_stop_impact[["target_pct", "stop_pct", "win_rate", "stop_rate", "expected_value", "median_return"]].to_string(index=False))

