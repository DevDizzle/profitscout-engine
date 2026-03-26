# DATA-CONTRACTS.md

## Purpose
Document the key data objects used by the current forward-trading workflow.

## Core source table
### `profitscout-fida8.profit_scout.overnight_signals_enriched`
Primary upstream table for V3 paper-trader selection.

Expected fields used by policy logic include:
- `scan_date`
- `ticker`
- `recommended_contract`
- `direction`
- `premium_score`
- `is_premium_signal`
- `recommended_volume`
- `recommended_oi`
- `recommended_dte`
- `recommended_spread_pct`
- `implied_volatility` / `recommended_iv` if available
- any market context fields needed for telemetry

## Forward ledger target
### Recommended target: `profitscout-fida8.profit_scout.forward_paper_ledger_v3`
This should be the clean V3 cohort table.

Minimum recommended columns:
- identity:
  - `scan_date`
  - `ticker`
  - `recommended_contract`
  - `direction`
- signal quality:
  - `is_premium_signal`
  - `premium_score`
- policy metadata:
  - `policy_version`
  - `policy_gate`
  - `is_skipped`
  - `skip_reason`
- context / telemetry:
  - `VIX_at_entry`
  - `SPY_trend_state`
  - `recommended_dte`
  - `recommended_volume`
  - `recommended_oi`
  - `recommended_spread_pct`
- execution:
  - `entry_timestamp`
  - `entry_price`
  - `target_price`
  - `stop_price`
  - `exit_timestamp`
  - `exit_reason`
  - `realized_return_pct`

## Cohort separation rule
Do not mix V2 and V3 observations without explicit policy metadata.

## Current policy contract
For V3, the intended cohort is:
- `premium_score >= 2`
- `recommended_volume > 250 OR recommended_oi > 500`

## Notes
- `VIX_at_entry` is retained as telemetry for later analysis, not as an execution gate.
- If the existing table name must be reused, add `policy_version` and `policy_gate` before trusting mixed data.
- If a code path still writes to `forward_paper_ledger`, treat that as a cleanup target unless it is intentionally preserved for backward compatibility.
