# RL Strategy Report: Predicting High-Velocity Options Wins

**Date:** 2026-01-23
**Objective:** Develop a dataset and Reinforcement Learning (RL) strategy to identify options contracts from the `winners_dashboard` that will achieve >50% gain within 3 days.

## 1. Executive Summary

To predict "explosive" options moves (>50% in <3 days), we are looking for a specific market regime: **High Gamma, High Momentum, and Volatility Expansion**.

The current architecture provides a solid foundation (`options_chain` has Greeks, `winners_dashboard` has initial signals). However, to train a high-performance RL agent, we must enrich the dataset with **derived second-order features** (e.g., Gamma Exposure, IV Rank) and high-fidelity **underlying technicals** (RSI velocity, ATR expansion) at the exact moment of the signal.

We recommend treating this primarily as a **Contextual Bandit** problem (stateless selection) or a **Short-Horizon MDP** (managing the trade tick-by-tick for 3 days), rather than a long-horizon portfolio optimization problem.

---

## 2. Feature Engineering: The "Explosive" Feature Set

The following features are critical for capturing the dynamics of a >50% move in a short timeframe.

### A. Options Market Structure (The "Fuel")
*Existing in `options_chain`: Delta, Gamma, Vega, Theta, IV, OI, Volume.*

**New Derived Features needed:**
1.  **Gamma Exposure (GEX):** `Gamma * Open Interest * 100`. High positive GEX acts as a magnet/stabilizer, while low/negative GEX can fuel volatility.
2.  **Vol/OI Ratio:** Volume divided by Open Interest. A spike (> 1.0) indicates aggressive new positioning (Smart Money flow).
3.  **IV Rank / IV Percentile:** Is the current IV low or high relative to the last 90 days? (Buy low IV, sell high IV).
4.  **Delta/Theta Ratio:** "Rent vs. Buy" metric. How much leverage are we getting per dollar of time decay?
5.  **Shadow Gamma:** (If available) The GEX of the *entire* chain, not just the contract. Helps identify "pinning" levels.

### B. Underlying Technicals (The "Spark")
*Current `winners_dashboard` has `outlook_signal`, but we need raw metrics.*

**New Features to Join (from `price_data`):**
1.  **RSI Velocity:** `RSI(t) - RSI(t-1)`. We want accelerating momentum, not just high momentum.
2.  **ATR Expansion:** `Current ATR / Average(ATR, 14)`. Is the stock starting to move faster than usual?
3.  **Distance to MA:** `(Price - SMA20) / Price`. Measures extension/mean-reversion potential.
4.  **Bollinger Band Squeeze:** Bandwidth is narrow (low volatility) just before an expansion.

### C. Macro/Sentiment (The "Wind")
1.  **Sector Momentum:** Relative strength of the stock's sector vs. SPY.
2.  **VIX Level:** Market-wide fear gauge.
3.  **Earnings Proximity:** Days until next earnings (avoid IV crush or gamble on it).

---

## 3. RL Environment Design

### Goal
Maximize the probability of `Max_Price(t+3d) >= Entry_Price * 1.50`.

### State Space (Observation)
The agent sees a vector $S_t$ for each candidate contract:
$$S_t = [\text{Greeks}, \text{LiquidityMetrics}, \text{UnderlyingTechnicals}, \text{TimeOfDay}, \text{DTE}]$$

*   **Normalization is key:** Greeks should be normalized by price or underlying price. Prices should be normalized to returns or log-differences.

### Action Space
**Option A: Selection Agent (Contextual Bandit)**
*   **Action:** `Binary {0, 1}` (Pass or Bet) or `Continuous [0, 1]` (Position Size/Confidence).
*   **Decision Frequency:** Once per signal generation.

**Option B: Management Agent (Sequential MDP)**
*   **Action:** `Discrete {Hold, Close_50%, Close_100%, Close_Full}`.
*   **Decision Frequency:** Hourly or Daily for the 3-day duration.

### Reward Function
The reward function must align strictly with the "50% gain" goal but punish risk (time decay).

$$R = \begin{cases} +10 & \text{if } \text{Return} \ge 50\% \\ \text{Realized\_Return} & \text{if } \text{Return} < 50\% \\ -1 \times \text{Time\_Decay\_Penalty} & \text{per step holding} \end{cases}$$

*   **Clip Rewards:** Cap downside to -100% (max loss) to prevent the agent from fearing volatility.
*   **Time Penalty:** Small negative reward per timestep to encourage quick wins (velocity of money).

---

## 4. Implementation Roadmap

### Phase 0: CRITICAL BLOCKER - Data Persistence
**Problem:** Currently, `options_chain_fetcher.py` and `options_candidate_selector.py` use `TRUNCATE` or `CREATE OR REPLACE` commands. This means we wipe our "Market State" history every 24 hours.
**Impact:** We have **zero** historical training data for the RL model (no historical Delta, Gamma, or IV). We cannot train an agent without history.
**Fix:**
1.  **Modify `options_chain_fetcher.py`:** Change `TRUNCATE` to an `INSERT` with a `fetch_date` partition.
2.  **Modify `options_candidate_selector.py`:** Change `CREATE OR REPLACE` to `INSERT` (append mode).
3.  **Create `options_chain_history`:** A partitioned table to store the raw "State" for every contract ever scanned.

### Phase 1: Dataset Construction (The "Gym")
We cannot train an RL agent on live data efficiently. We need an **Offline RL Dataset**.

1.  **Snapshot Replay:** Modify `options_chain_fetcher` or create a new job to save "Full Chain Snapshots" for every `winners_dashboard` signal.
2.  **Outcome Labeling:** Create a `training_labels` table.
    *   For every signal $S$ at time $T$, query `price_data` (or minute-level options data if available) for $T$ to $T+3days$.
    *   Compute `Max_Return`, `Time_To_Max_Return`, `Drawdown`.
3.  **Join:** Create a flattened table `rl_training_set`:
    *   `[Features_at_T] -> [Outcome_at_T+3d]`

### Phase 2: Model Selection
1.  **Baseline (Supervised):** Train a Gradient Boosted Tree (XGBoost/LightGBM) to predict `Probability(Max_Return > 50%)`. This identifies feature importance quickly.
2.  **RL Agent:** Use **PPO (Proximal Policy Optimization)** or **DQN (Deep Q-Network)**.
    *   Use **Stable-Baselines3** (standard Python RL library).
    *   Create a custom Gym Environment that loads rows from `rl_training_set`.

### Phase 3: The "Paper Trade" Loop
1.  Deploy the model as a "Filter" in the `options_candidate_selector` pipeline.
2.  Log the Model's "Confidence Score" alongside the standard signals.
3.  Track "Model vs. Baseline" performance in `performance_tracker`.

## 5. Recommended Next Steps

1.  **Create the Dataset Pipeline:** Write a script to backfill `features + outcomes` for all existing rows in `winners_dashboard` using historical `price_data`.
2.  **Calculate Missing Features:** Implement `RSI_Velocity` and `ATR_Expansion` in the `technicals_analyzer`.
3.  **Prototype with XGBoost:** Before full RL, verify that the features actually contain signal for >50% moves.
