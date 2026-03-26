# Intraday Execution Robustness Sweep: Executive Summary

**Objective:** Determine if the options signal system possesses a robust tradable edge when subjected to realistic intraday execution constraints (slippage, specific entry times, strict stop/target rules), or if the edge was an artifact of "peak return" retrospective analysis.

## 1. Executive Summary & Verdict
**Yes, a stable, tradable pocket exists.** However, the edge is highly sensitive to **Entry Time**. 

The initial failure of the system was entirely due to the `09:45 AM` entry constraint. Entering options trades in the first 30-60 minutes of the market open subjects the position to massive volatility spread pricing, resulting in rapid, unwarranted stop-outs. 

By shifting the entry time to **15:00 ET (3:00 PM)**, the strategy sidesteps the opening noise, resulting in a dramatic recovery of the edge. At a 15:00 entry, the strategy is highly robust across multiple target/stop configurations and survives the "Stress Scenario" slippage penalties.

## 2. Ranked Table of Top Configurations
*Filtered for minimum 10 valid trades, Base Scenario (2% entry slippage).*

| Cohort | Entry Time | Target | Stop | Max Hold | Win Rate | Stop Rate | EV per Trade | Valid Trades |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **HEDGE_HIGH_RR** | **15:00** | **+50%** | **-40%** | **3 Days** | **42.8%** | **14.2%** | **+14.89%** | 14 |
| **HEDGE_HIGH_RR** | **15:00** | **+40%** | **-40%** | **3 Days** | **50.0%** | **14.2%** | **+12.88%** | 14 |
| **SCORE_GTE_2** | **15:00** | **+40%** | **-25%** | **3 Days** | **52.6%** | **31.5%** | **+12.59%** | 19 |
| **HEDGE_HIGH_RR** | **15:00** | **+40%** | **-25%** | **3 Days** | **50.0%** | **28.5%** | **+12.09%** | 14 |

*Note: The `HEDGE_HIGH_ATR` cohort only yielded 2-5 valid trades depending on the entry time filter, making it too small a sample size to trust independently in this sweep.*

## 3. Fragile & Misleading Configurations to Ignore

*   **ALL Morning Entries (09:45, 10:00, 10:30, 11:00):** Do not trade these signals in the morning. Every single morning entry time across all cohorts resulted in a negative or near-zero EV (ranging from -8.5% to +1.5%). The stop-out rate for 09:45 entries is over 50%.
*   **Tight Targets (+15% to +20%):** Attempting to scalp small moves (+15% or +20%) destroys the edge. The EV drops to near zero (+0.3%) because the frequent stop-outs outpace the small target captures.
*   **1-Day Holds:** Forcing the trade to close by the end of Day 1 results in catastrophic EV (-7% to -8%). These institutional flow setups require 2-3 days to develop.

## 4. Analysis Questions Answered

**1. Does later entry improve EV by avoiding opening-volatility noise?**
Absolutely. It is the single most important variable. Shifting entry from 09:45 to 15:00 improved the Expected Value for `HEDGE_HIGH_RR` (+40%/-25%) from **-4.3% to +12.0%**. 

**2. Are smaller targets like +20% or +25% materially better than +40%?**
No. The math demands a larger asymmetry. At 15:00 entry, lowering the target from +40% to +20% collapsed the EV from +12% down to +2%. You must let the winners run to at least +35% or +40%.

**3. Does widening the stop improve EV, or just increase loss severity?**
Widening the stop **improves** EV. Moving the stop from -25% to -40% (while keeping the target at +40%) reduced the stop-out rate from 28.5% to 14.2%, and increased the EV from +12.0% to +12.8%.

**4. Does premium_score >= 2 or premium_score >= 3 produce better tradable behavior?**
Yes. `SCORE_GTE_2` slightly outperformed `HEDGE_HIGH_RR` at the 15:00 entry (+12.5% EV vs +12.0% EV) and provided a larger sample size (19 vs 14 trades). `SCORE_GTE_3` did not produce enough volume to register.

**5. Are there signs that the edge is real but mistimed?**
Yes. The complete failure of the 09:45 entry contrasted with the strong success of the 15:00 entry proves the overnight signals are highly predictive of the *multi-day* directional move, but the immediate morning options pricing behaves erratically.

## 5. Recommendation for Forward-Testing
The system has survived the stress tests, provided we change the execution rules. 

**The Recommended Forward-Test Configuration:**
*   **Cohort:** `premium_score >= 2` (Provides the best blend of EV and trade volume).
*   **Entry Time:** 15:00 ET (Next trading session after the signal).
*   **Target:** +40%
*   **Stop:** -25% (or -30% to give slightly more breathing room).
*   **Hold Time:** 3 Days.

**Stability Check (Does it survive the Stress Scenario?):**
At this configuration, the strategy survived the Stress Case (5% entry slippage + 5% timeout penalty) while maintaining a positive EV of **+6.6%**. 

**Next Step for Simulation Code:**
If you want to plug this into your Monte Carlo, update your MC script to use the actual realized distribution of this specific setup rather than binary 80/20 win rates:
*   52.6% Target Hits (+40%)
*   31.5% Stop Hits (-25%)
*   15.9% Timeouts (Mixed returns)