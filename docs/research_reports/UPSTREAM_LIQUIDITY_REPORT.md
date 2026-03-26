# Upstream Liquidity Gate Analysis: Salvaging Path A

## Executive Summary
**Is Path A still alive?** Yes, but barely. The edge is highly dependent on combining the right entry time (15:00 ET) with strict pre-enrichment liquidity filtering.

**Does an upstream liquidity gate materially improve the system?** Yes. Applying an upstream gate based on Volume and Open Interest dramatically reduces the "invalid liquidity" failure rate (from ~60% down to ~43%). Furthermore, filtering out low-liquidity options *does not destroy the alpha*; in fact, the directional win rates of the signals that pass the liquidity filters remain exceptionally high (80%+ in directional peak return).

**What specific gate looks best right now?**
The optimal gate is: `(recommended_volume >= 100 OR recommended_oi >= 250)` on the `premium_score >= 2` cohort.

## Ranked Candidate Liquidity Gates
*Analysis conducted on the `SCORE_GTE_2` cohort (Premium Score >= 2). Base population: 45 signals.*

| Gate Definition | Pass Count | Pass Rate | Est. Monthly Trades | Avg Spread | Avg Vol | Avg OI | Dir. Win Rate | Avg Peak Return 3D |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **None (Baseline)** | 45 | 100.0% | 45.0 | 19.3% | 1,436 | 767 | 80.0% | +4.96% |
| **V>25 \| OI>50** | 36 | 80.0% | 36.0 | 21.5% | 1,792 | 958 | 75.0% | +4.04% |
| **V>50 \| OI>100** | 35 | 77.8% | 35.0 | 22.1% | 1,842 | 985 | 77.1% | +4.23% |
| **V>100 \| OI>250** | 30 | 66.7% | 30.0 | 25.2% | 2,140 | 1,144 | **80.0%** | **+4.48%** |
| **Spread <= 15%** | 23 | 51.1% | 23.0 | 6.3% | 423 | 95 | 87.0% | +4.98% |
| **V>50\|OI>100 & Spread<=15%**| 14 | 31.1% | 14.0 | 6.0% | 685 | 154 | 85.7% | +4.98% |

*Note: Spread-based gates severely bottleneck the monthly trade count (down to 14/month) and resulted in a massive 83% invalid-liquidity rejection rate during the execution test, indicating that tight spreads at night do not guarantee liquidity at 15:00 the next day.*

## Execution Viability Test Results (Constrained Rule: 15:00 / +40 / -25 / 3D)

**Test 1: Gate = `V>25 | OI>50`**
*   **Total Passing Signals:** 37
*   **Invalid Liquidity Skips in Execution:** 18 (48.6%)
*   **Valid Executable Trades:** 19
*   **Realized Win Rate:** 52.6%
*   **Realized Stop Rate:** 31.6%
*   **Realized EV:** **+12.59% per trade**

**Test 2: Gate = `V>100 | OI>250`**
*   **Total Passing Signals:** 30
*   **Invalid Liquidity Skips in Execution:** 13 (43.3%)
*   **Valid Executable Trades:** 17
*   **Realized Win Rate:** 47.1%
*   **Realized Stop Rate:** 35.3%
*   **Realized EV:** **+9.37% per trade**

## Recommended Production Candidate
Path A survives if we implement the following:

**Upstream Filter Definition:** `(recommended_volume >= 100 OR recommended_oi >= 250)`
**Target Cohort:** `premium_score >= 2`
**Execution Rule:** 15:00 ET Entry | +40% Target | -25% Stop | 3-Day Max Hold

**Implementation Notes:**
*   This should **augment** the `is_tradeable` flag. The system should only process and enrich signals that pass this upstream volume/OI gate.
*   **Is it good enough for forward paper trading?** Yes. By gating out low-volume contracts upfront, we bring the invalid liquidity execution rate down from ~60% to ~43%. While 43% is still high, the remaining executable trades still yield a positive Expected Value (+9.37%) without the edge collapsing.

We must accept that even with upstream gating, roughly 4 out of 10 signals will simply not have enough liquidity to fill safely at 15:00 the next day. The strategy is to completely ignore those and only execute the 6 out of 10 that do.