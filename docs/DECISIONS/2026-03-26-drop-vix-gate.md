# Decision: drop macro VIX gating for forward paper trading

- **Date:** 2026-03-26
- **Status:** accepted for forward validation

## Context
Recent backtesting against the historical `overnight_signals_enriched` dataset suggested that premium signals with `premium_score >= 2` remain strong even without a macro VIX gate. The prior V2 regime logic was likely suppressing valid winners by double-penalizing setups that were already filtered by options-market structure.

## Decision
Adopt a V3 forward-validation posture that:
- removes the macro VIX gate from eligibility
- keeps `premium_score >= 2`
- uses the sweet-spot liquidity gate:
  - `recommended_volume > 250 OR recommended_oi > 500`

## Rationale
- premium score appears to be the stronger core signal-quality control
- liquidity is a more direct execution-quality proxy than broad VIX regime exclusion
- clean forward validation requires a simpler, explicit policy

## Consequences
- historical V2 research docs remain useful context, but are superseded as execution spec
- forward ledger cohorts should be separated or versioned clearly
- VIX can remain as telemetry for analysis, but not as a skip criterion in V3

## Follow-up
- keep `forward-paper-trader/main.py` aligned to the V3 liquidity-only policy
- write forward runs into a versioned ledger target with explicit policy metadata
- validate one clean V3 cohort out of sample before making stronger claims
