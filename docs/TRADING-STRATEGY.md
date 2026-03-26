# TRADING-STRATEGY.md

## Status
Current canonical strategy doc for forward paper trading.

## Objective
Validate a simple, high-conviction forward paper-trading policy that preserves alpha from the overnight options-flow scoring system without double-penalizing signals with a macro VIX gate.

## Current policy: V3 Liquidity-Only Forward Validation

### Eligibility
A signal is eligible for forward paper execution when:
- `premium_score >= 2`
- `recommended_volume > 250 OR recommended_oi > 500`

### Explicit non-rule
- **No macro VIX gate** in the current V3 policy.
- VIX may still be logged as telemetry/context, but it is not a reason to skip a trade.

### Deduplication
- One trade per `ticker` per `scan_date`.

### Intended purpose
This policy exists to answer a narrow question:
> Does dropping the macro VIX gate while keeping only high-conviction, sufficiently liquid premium signals produce better forward results out of sample?

## Execution defaults
Unless a newer decision doc says otherwise, the current execution framing remains:
- entry around market open / configured paper-entry time
- fixed target/stop brackets
- max hold of 3 trading days

These mechanics should be treated as implementation defaults, not sacred doctrine.

## What changed from V2
### V2 thesis
- premium score plus macro regime awareness
- VIX-based gating / skip logic
- more conservative filtering designed to avoid bad market regimes

### V3 thesis
- premium score already captures enough options-market information that hard VIX gating may suppress real winners
- the right control knob is **signal quality + liquidity**, not broad macro disqualification

## What should be logged
Every ledger row should preserve enough information to analyze the policy later:
- `policy_version`
- `policy_gate`
- `scan_date`
- `ticker`
- `recommended_contract`
- `direction`
- `premium_score`
- `recommended_volume`
- `recommended_oi`
- `recommended_dte`
- `VIX_at_entry` (telemetry only)
- execution prices / timestamps
- skip status and skip reason

## Current validation posture
- Keep V2 and V3 cohorts separate.
- Do not mix policy runs in one ambiguous ledger without version metadata.
- Favor clean forward evidence over short-window backtest confidence.

## Open questions
- Should `recommended_dte` remain a hard filter or only logged as telemetry?
- Should spread quality later become part of the gate?
- Should the sweet-spot gate itself be split into “broad” and “sniper” sub-cohorts for reporting?
