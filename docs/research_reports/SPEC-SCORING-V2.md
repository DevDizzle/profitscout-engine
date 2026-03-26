# SPEC: Regime-Aware Options Execution Policy (v2)

## 1. Overview
This specification defines the "v2" execution policy for the overnight options signal system. It replaces the previous rigid, mechanical execution assumptions with a **regime-aware execution layer**. 

The core philosophy of v2 is that the underlying signals possess directional alpha, but translating that alpha into options profitability requires strict pre-trade liquidity gating and regime-specific "skip conditions" to avoid catastrophic stop-out environments (e.g., high VIX, short DTE).

*(Note: Prior discussions regarding "industry cluster boosts" for the overnight scanner belong to a separate, upstream signal generation spec and are not covered here).*

## 2. Research Background
Rigorous intraday backtesting and chronological holdout analysis of the v1 mechanical strategy revealed:
*   **09:45 AM Entry Failure:** Early morning execution was universally unprofitable due to extreme bid/ask spread volatility.
*   **Liquidity Fragility:** Over 60% of raw generated signals were untradable in live conditions due to zero intraday options volume.
*   **Regime Fragility:** Even after moving entry to 15:00 ET and filtering for liquidity, the fixed execution bracket (+40% / -25%) suffered a total collapse during out-of-sample holdout testing (shifting from +27% EV in Feb to -6% EV in March).
*   **Clustered Failure Modes:** Deep pattern analysis showed that stop-outs clustered heavily when VIX was elevated (>25) and when contract duration was too short (<14 DTE).

## 3. Eligibility Rules (Upstream Gating)
Before a signal can even be considered for the execution layer, it must pass these strict upstream eligibility filters.

*   **Premium Requirement:** `premium_score >= 2`
*   **Directional Alignment:** `direction == 'BEARISH'` (The current iteration of the system is strictly optimized for bearish flow due to prevailing signal distribution).
*   **Liquidity Gate:** `recommended_volume >= 100 OR recommended_oi >= 250`

*If a signal fails these rules, it is silently ignored by the execution engine.*

## 4. Skip Conditions (Regime Gating)
If a signal passes the Eligibility Rules, the system must evaluate the active market regime at the time of intended execution. The system will **SKIP** the trade (no entry) if *any* of the following conditions are met:

*   **VIX Danger Zone:** `VIX > 25.0` at the time of entry evaluation.
*   **Theta Burn Zone:** `recommended_dte < 14` days.

*(Note: Future iterations may include SPY Trend conflict rules, but they are currently excluded to preserve sample size).*

## 5. Execution Rules
If a signal passes both the Eligibility Rules and avoids all Skip Conditions, it will be executed under the following strict mechanical bracket:

*   **Entry Time:** Next valid trading session at exactly **15:00 ET (3:00 PM)**.
*   **Entry Price:** Assumed `15:00 ET raw close + 2% slippage penalty`.
*   **Target (Take Profit):** **+40%** from the slippage-penalized entry price.
*   **Stop Loss:** **-25%** from the slippage-penalized entry price.
*   **Max Hold:** Exit at the market close (15:59 ET) on the **3rd trading session** after entry (Timeout).
*   **Intrabar Rule:** If Target and Stop are hit in the same 1-minute candle, assume the Stop was hit first.

## 6. Logging Requirements (Forward Paper Ledger)
To validate this policy out-of-sample, a new BigQuery table (`profitscout-fida8.profit_scout.forward_paper_ledger`) must be created to log all intended executions. 

**CRITICAL:** Skipped trades *must* be logged to verify the skip logic is firing correctly and to backtest missed opportunities.

The ledger schema must include:

**Identity**
*   `scan_date` (DATE)
*   `ticker` (STRING)
*   `recommended_contract` (STRING)
*   `direction` (STRING)

**Policy State**
*   `is_skipped` (BOOLEAN)
*   `skip_reason` (STRING, Nullable) - Must be populated if skipped (e.g., "VIX_GT_25", "DTE_LT_14").

**Market / Regime Context**
*   `VIX_at_entry` (FLOAT)
*   `SPY_trend_state` (STRING) - "BULLISH" or "BEARISH" relative to 10-day SMA.
*   `recommended_dte` (INTEGER)
*   `recommended_volume` (INTEGER)
*   `recommended_oi` (INTEGER)
*   `recommended_spread_pct` (FLOAT)

**Execution**
*   `entry_timestamp` (TIMESTAMP, Nullable)
*   `entry_price` (FLOAT, Nullable)
*   `target_price` (FLOAT, Nullable)
*   `stop_price` (FLOAT, Nullable)
*   `exit_timestamp` (TIMESTAMP, Nullable)
*   `exit_reason` (STRING, Nullable) - Enum: `TARGET`, `STOP`, `TIMEOUT`, `INVALID_LIQUIDITY`, `SKIPPED`
*   `realized_return_pct` (FLOAT, Nullable)

## 7. Validation Protocol
This policy is currently in an R&D forward-testing phase. 

**Re-evaluation Trigger:** The policy and its performance will only be re-evaluated after **30 forward paper trades** (where `is_skipped = FALSE`) or **30 calendar days**, whichever comes later.

**Validation Rules:**
1.  **Frozen Policy:** There will be zero midstream rule changes during the validation window.
2.  **No Loosening:** We will not loosen skip filters or eligibility gates simply to force more trade volume.
3.  **No Bracket Tweaking:** The +40% / -25% bracket is locked until the validation window closes.

*The objective is to collect unpolluted, out-of-sample evidence.*

## 8. Current Status / Risk Statement
*   **Live Capital:** **NO.**
*   **Monte Carlo Modeling:** **NO.**
*   **Production Ready:** **NO.**
*   **Forward Paper Trading:** **YES.**

The current system has demonstrated severe regime-fragility in historical holdouts. Until the v2 skip logic proves it can survive shifting regimes out-of-sample, the system is deemed unsafe for live capital.

## 9. Future Extensions
If the v2 policy successfully survives the validation protocol, Phase 3 will introduce adaptive, multi-bracket execution (e.g., widening the stop and lowering the target dynamically in medium-volatility environments rather than a binary skip/trade decision).

---

### Implementation Checklist
- [ ] Overwrite existing `SPEC-SCORING-V2.md` with this document.
- [ ] Create `forward_paper_ledger` table schema in BigQuery.
- [ ] Build automated daily paper-trading script that applies the eligibility rules, skip conditions, and simulated 15:00 execution.
- [ ] Schedule script to run daily at 16:30 ET to log the day's paper outcomes.