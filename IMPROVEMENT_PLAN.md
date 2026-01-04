# ProfitScout Engine Improvement Plan

This document tracks planned improvements to the ProfitScout trading engine, focusing on optimizing contract selection and signal quality for High Gamma strategies.

## 1. Contract Selection Logic (Completed)
- **Problem:** "ML Sniper" logic was picking deep OTM contracts (e.g., $320 Call on $226 Stock) because of high AI scores and volume, despite being unrealistic "lotto tickets".
- **Fix:** Added Moneyness Guardrails to `options_candidate_selector.py`.
  - **Calls:** Strike must be 90-120% of Stock Price.
  - **Puts:** Stock Price must be 90-120% of Strike Price.
- **Status:** ✅ Applied on 2026-01-04.

## 2. Volatility Analysis Heuristics (Completed)
- **Problem:** The current definition of "Expensive" IV (> 1.2x HV30) is too strict for High Gamma/Explosive setups. Breakout stocks naturally command higher premiums. Flagging them as "Expensive" hurts the "Strong" signal quality.
- **Proposed Change:** Loosen the "Cheap" and "Expensive" thresholds in `options_analyzer.py`.
  - **Current:** Cheap < 0.90, Expensive > 1.20.
  - **Proposed:** Cheap < 0.85, Expensive > 1.50 (allows 50% premium over HV before flagging).
- **Status:** ✅ Applied on 2026-01-04.

## 3. Future Optimization Areas
- **Candidate for Review:**
  - Are we penalizing "Theta" too heavily for short-term Sniper trades?
  - Should "Spread Pct" limits be dynamic based on DTE? (e.g., tighter spreads required for shorter DTE).
