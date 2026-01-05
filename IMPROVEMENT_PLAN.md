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

## 3. Risk Management Overhaul (Completed)
- **Problem:** Review identified excessive risk in liquidity (spreads up to 50%), duration (DTE as low as 5), and "forgiveness" logic that ignored red flags for certain tiers.
- **Fix:** Implemented strict commercial-grade risk parameters in `options_candidate_selector.py` and `options_analyzer.py`.
  - **Liquidity:** Global max spread **15%** (was 50%). Min Volume **100** (was 50).
  - **Duration:** Global min DTE **14 days** (was 5).
  - **Safety:** Removed "immunity" for ML Snipers and Rip Hunters.
    - ML Snipers are now downgraded to "Weak" if they fail checks.
    - Rip Hunters are rejected if IV is Expensive (>1.5x HV) or Spread is bad.
  - **Projections:** Reset Expected Move `haircut` to **0.75** (was 1.0) for conservative targeting.
- **Status:** ✅ Applied on 2026-01-04.

## 4. News Intelligence Upgrade (Completed)
- **Problem:** `news_analyzer` was treating old news as equal to fresh news and occasionally hallucinating vague "bullish" sentiments without data.
- **Fix:** Updated LLM Prompt in `news_analyzer.py`.
  - **Recency:** Explicit instruction to prioritize last 48h; treat >3d as noise.
  - **Evidence:** Mandatory citation of specific numbers (EPS, Rev, Guidance) and dates.
  - **Status:** ✅ Applied on 2026-01-04.

## 5. Widen the Net (Candidate Selection) (Completed)
- **Problem:** Previous risk controls (Section 3) were too aggressive, filtering out over 99.9% of contracts (only ~7 survivors from 300k+).
- **Fix:** Relaxed specific constraints in `options_candidate_selector.py` to allow high-quality setups that were marginally failing strict filters.
  - **Liquidity:** Global max spread increased to **25%** (was 15%).
  - **Volume:** ML Snipers & Rip Hunters now accepted with Volume > **50** OR Open Interest > **500** (was Vol > 100 strictly).
  - **Duration:** Global min DTE reduced to **7 days** (was 14) to capture explosive gamma moves.
  - **Moneyness:** Rip Hunters range widened to **0.90 - 1.20** (was 1.00 - 1.15) to allow ITM plays.
- **Status:** ✅ Applied on 2026-01-05.