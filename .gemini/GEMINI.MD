# GEMINI.MD — GammaRips Engine

## Purpose
Canonical root context for this repo. For Gemini projects, this file is the primary bootstrap and should be treated as the top-level source of project guidance.

## Mission
GammaRips Engine is the active backend and research workspace for overnight options-flow scanning, enrichment, reporting, paper execution, and performance tracking. The current operating goal is to validate and improve the forward paper-trading policy so it can become a reliable income-supporting engine.

## Read first
Before making meaningful changes, read in this order:
1. `docs/TRADING-STRATEGY.md`
2. `docs/ARCHITECTURE.md`
3. `docs/DATA-CONTRACTS.md`
4. `docs/TESTING.md`
5. `docs/DECISIONS/2026-03-26-drop-vix-gate.md`
6. `docs/EXEC-PLANS/v3-reset.md`

## Current policy posture
- The current working policy is **V3 liquidity-only forward validation**.
- The macro VIX gate is considered dropped unless explicitly reintroduced by a documented decision.
- The current desired cohort is:
  - `premium_score >= 2`
  - `recommended_volume > 250 OR recommended_oi > 500`
- Historical V2 regime docs may still exist, but they are not the current execution source of truth.

## Ground rules
- Treat `docs/TRADING-STRATEGY.md` as the source of truth for current execution policy.
- Treat `docs/research_reports/` as historical research, not automatically authoritative.
- Do not reintroduce VIX gating without an explicit documented decision.
- Do not mix V2 and V3 forward-ledger cohorts.
- Prefer archival over deletion when cleaning old research artifacts.
- Never hardcode API keys or secrets in source.

## Editing rules
- Do not trust historical `PROMPT-*` docs or old research summaries as the current implementation spec.
- When touching ledger logic, keep cohort/version metadata explicit.
- If changing execution policy, update `docs/TRADING-STRATEGY.md` and add a decision note in `docs/DECISIONS/`.
- Prefer minimal, reversible changes when cleaning old scripts.

## Role split
### Engineering mode
Use for:
- service cleanup
- refactors
- deployment fixes
- testing
- BigQuery / Firestore integration
- documentation alignment

### Trading research mode
Use for:
- cohort analysis
- backtests
- policy comparisons
- experiment logs
- thesis updates

## Repo intent by area
- `forward-paper-trader/` = execution and forward validation
- `src/` and `overnight-scanner/` = scanner logic
- `enrichment-trigger/` = enrichment pipeline
- `agent-arena/` = debate / model selection layer
- `win-tracker/` = post-trade outcome tracking
- `backtesting_and_research/` = research code and generated artifacts
- `docs/` = authoritative project docs plus archived historical research

## Current high-priority work
- Stabilize V3 forward-paper policy around `premium_score >= 2` plus the sweet-spot liquidity gate.
- Keep the repo scaffold clean enough that coding agents and Gemini can navigate it without relying on stale docs.
- Preserve explicit V3 cohort metadata in the forward ledger and avoid policy drift back toward undocumented VIX gating.

## Important caution
Treat `docs/TRADING-STRATEGY.md` plus the current `forward-paper-trader/main.py` implementation as the execution source of truth. If policy changes again, update both the docs and the code in the same pass.
