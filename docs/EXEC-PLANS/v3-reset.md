# V3 reset plan

## Goal
Reset GammaRips forward paper trading around the new liquidity-only V3 policy while leaving a clear audit trail of what changed.

## Scope for Pass A
- create root agent harness
- create Gemini repo context
- create canonical strategy/docs
- prepare SQL reset plan
- archive stale/generated non-code clutter
- defer final code trust review for `forward-paper-trader/main.py`

## Scope for Pass B
- inspect and clean `forward-paper-trader/main.py`
- align table target and schema
- verify scheduler/runtime assumptions
- run a dry or limited validation pass

## V3 target policy
- `premium_score >= 2`
- `recommended_volume > 250 OR recommended_oi > 500`
- no macro VIX skip logic
- separate V3 cohort from historical V2 data

## Success criteria
- repo has one obvious canonical strategy doc
- repo has one obvious agent bootstrap path
- repo has a concrete SQL reset file ready
- stale docs no longer masquerade as current instructions
- code review of trader remains explicitly pending, not silently assumed complete
