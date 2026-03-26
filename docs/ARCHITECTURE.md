# ARCHITECTURE.md

## Purpose
High-level map of the active GammaRips Engine system.

## Active components
### `src/` / scanner core
Core scoring and overnight signal generation.

### `overnight-scanner/`
Scanner-facing package / service wrapper for market-wide overnight options flow scanning.

### `enrichment-trigger/`
Enrichment service for news, technicals, and AI-generated context.

### `overnight-report-generator/`
Daily report generation for the overnight signal set.

### `agent-arena/`
Multi-model debate / consensus service for ranking or adjudicating signal quality.

### `forward-paper-trader/`
The service/script responsible for creating a forward paper-trading cohort from enriched overnight signals.

### `win-tracker/`
Tracks realized performance after the trade window and closes the loop on execution outcomes.

### `backtesting_and_research/`
Research scripts and generated artifacts for studying filters, execution assumptions, and cohort behavior.

## Data flow
1. Overnight scanner produces signal candidates.
2. Enrichment adds news/technical/context features.
3. Optional report/arena layers add synthesis.
4. Forward paper trader selects a policy cohort and writes intended executions.
5. Win tracker measures post-entry outcomes.

## Current architecture truth
The most important architectural boundary right now is between:
- **signal generation research**
- **execution policy selection**
- **outcome measurement**

Those must stay separable so policy changes can be evaluated cleanly.

## Historical areas
- `_archive/` contains older legacy code and should not be treated as active runtime infrastructure.
- `docs/research_reports/` contains historical research and planning context, not necessarily the current execution spec.

## Current technical debt
- execution logic and docs have drifted
- no prior root agent harness
- generated artifacts and historical prompts were cluttering the repo
- policy versioning in the forward ledger needs to be explicit

## Near-term objective
Make the repo legible enough that a coding agent or Gemini can safely operate without guessing which documents are authoritative.
