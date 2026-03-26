# Monte Carlo Simulation Plan Update

**TLDR Verdict:** Yes, we have a tradable edge, but only if we delay the entry until 3:00 PM (15:00 ET) to avoid morning volatility, resulting in an expected value of +12.6% per trade. 

To validate the safety of scaling capital, you need to update your Monte Carlo simulation using the newly discovered realistic live distribution rather than the old theoretical one.

## New Monte Carlo Parameters

Instead of simulating a binary 80% Win / 20% Loss, the new Monte Carlo will use a weighted 3-outcome distribution based on the `SCORE_GTE_2` cohort entering at 15:00 ET with a +40% / -25% bracket.

**The Trade Distribution:**
*   **Target Hits (Win):** 52.6% probability -> Return: +40%
*   **Stop Hits (Loss):** 31.5% probability -> Return: -25%
*   **Timeouts (Stagnant):** 15.9% probability -> Return: Use median timeout return (approximately +5.0%) or simulate a random draw between -25% and +40%. (For simplicity, a flat +5% or 0% is conservative and accurate to the ledger data).

**The Account Mechanics (Unchanged):**
*   **Starting Capital:** $2,500
*   **Runway Phase (Months 1-3):** Scale bets ($1k -> $1.5k -> $2k) with zero withdrawals.
*   **Harvest Phase (Months 4-8):** Flat $2,500 bet size with $3,000 withdrawn every 8 trades.
*   **Frequency:** 8 sequential trades per month.
*   **Simulations:** 10,000 lifetimes to calculate exact Risk of Ruin.