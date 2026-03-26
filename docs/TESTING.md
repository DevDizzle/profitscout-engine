# TESTING.md

## Goal
Provide a minimal validation checklist for changes to GammaRips execution policy and ledger-writing logic.

## Before policy changes
- confirm the canonical policy in `docs/TRADING-STRATEGY.md`
- confirm target ledger table name and schema in `docs/DATA-CONTRACTS.md`
- verify whether current code still points at an older ledger table

## Validation checklist for forward-paper-trader
### 1. Static sanity
- verify no hardcoded secrets remain
- verify policy constants match the documented strategy
- verify table target is the intended V3 table or explicitly versioned target

### 2. Query sanity
- run a read-only query against `overnight_signals_enriched`
- confirm the cohort selected by code matches:
  - `premium_score >= 2`
  - `recommended_volume > 250 OR recommended_oi > 500`

### 3. Dedup sanity
- verify only one row per `ticker` per `scan_date` is eligible for execution

### 4. Ledger write sanity
- verify rows include:
  - `policy_version`
  - `policy_gate`
  - `is_skipped`
  - `skip_reason`
- verify VIX is logged as telemetry, not used as a skip reason in V3

### 5. Dry-run / limited run
- run a dry test or one-day cohort simulation before trusting the next live scheduled run
- inspect written rows manually

### 6. Outcome tracking sanity
- verify downstream win tracking still resolves against the chosen ledger target and contract identity fields

## Recommended first-morning review
The current `forward-paper-trader/main.py` should get a deliberate cleanup review before it is trusted. Focus on:
- stale VIX logic
- table naming
- policy metadata
- secret hygiene
- skip-reason correctness
