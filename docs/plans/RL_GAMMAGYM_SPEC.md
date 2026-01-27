# RL Environment Design: "GammaGym" (Active Management)

## 1. Objective
Design a Reinforcement Learning environment to train an autonomous agent that trades high-velocity options contracts. Unlike simple classification tasks, this environment requires the agent to manage the entire lifecycle of the trade (Entry, Holding, and Exit).

The goal is to solve the **"Ride the Winner"** problem: The agent must learn to hold winning positions as long as momentum accelerates (uncapped upside), rather than exiting at an arbitrary fixed percentage, while strictly managing Theta decay and downside risk.

## 2. The Prompt (Instructions for the AI)
"You are a Quantitative AI Engineer. Your task is to implement a custom OpenAI Gym environment class named `GammaGym` that simulates an options trading session.

### The Environment Logic:
The environment steps through historical 1-minute market snapshots. At each step, the agent manages a single slot for an options contract.

#### 1. Action Space (Discrete):
*   `0` (**WAIT / HOLD**): If flat, do nothing. If long, keep holding.
*   `1` (**BUY**): Enter a position at the current Ask price. (Penalty if already long).
*   `2` (**SELL**): Exit the position at the current Bid price. (Penalty if not long).

#### 2. Observation Space (State):
A vector of normalized float32 values representing the market state:
*   **Greeks:** $\Delta$ (Delta), $\Gamma$ (Gamma), $\Theta$ (Theta), $\nu$ (Vega).
*   **Dynamics:** IV Rank, Bid-Ask Spread, Vol/OI Ratio.
*   **Position State:** `Current_Unrealized_PnL`, `Time_Since_Entry`.

#### 3. Reward Function (Crucial):
You must implement a **Dense Mark-to-Market Reward** scheme to solve the credit assignment problem. Do not use sparse rewards (rewarding only at the end).

$$R_t = (V_t - V_{t-1}) - (\lambda \times \text{TimeDecay})$$

Where $V_t$ is the value of the position at step $t$.
*   If the agent is flat (no position), $R_t = 0$.
*   If the agent holds a winning position, it receives positive rewards every step the price climbs.
*   If the price stagnates, the $\lambda$ (Theta penalty) turns the reward negative, forcing the agent to sell.

### Deliverable:
Submit a Python file `gamma_gym.py` containing the class and a training script `train_agent.py` that uses PPO (Proximal Policy Optimization) to solve the environment."

## 3. The Data (Provided in VM)
The virtual machine contains `options_market_replay.parquet`, a high-fidelity dataset structured for sequential decision making:
*   **Index:** timestamp (1-minute intervals).
*   **Context:** `contract_id` (The specific option being traded).
*   **Features:** `delta`, `gamma`, `theta`, `iv`, `underlying_price`, `rsi_velocity`, `atr_expansion`.
*   **Pricing:** `bid_price`, `ask_price`, `mid_price`.
*   **Metadata:** `regime_label` (e.g., "Low Volatility", "Gamma Squeeze" — used for the Judge's evaluation, hidden from Agent).

## 4. The Judge (Automated Verification)
The Judge evaluates the agent's ability to "let winners run" while cutting losers fast.

*   **Test 1: Reward Function Sanity (The "Lazy Agent" Trap)**
    *   The Judge runs a dummy agent that never trades.
        *   **Pass Condition:** Total Reward must be exactly 0.
    *   The Judge runs a dummy agent that buys and holds forever in a flat market.
        *   **Pass Condition:** Total Reward must be negative (checking that Time Decay penalty $\lambda$ is active).

*   **Test 2: Dynamic Exit Capability (The "Uncapped" Test)**
    *   The Judge runs the trained agent on a specific "Moonshot" dataset where a contract goes +50%, then +100%, then +300% before crashing.
    *   **Pass Condition:** The agent must **NOT** sell at +50%. It must hold until the momentum breaks (e.g., exiting at +200% or higher).
    *   **Fail Condition:** Agent sells early (leaving profit on the table) or holds through the crash (ignoring risk).

*   **Test 3: Generalization Score**
    *   The agent is evaluated on 1,000 unseen episodes.
    *   **Metric:** $$Score = \text{Total\_Return} \times \log(\text{Sortino\_Ratio})$$
    *   **Pass Threshold:** Score > Baseline (where Baseline is a static "Take Profit at 50% / Stop Loss at 20%" strategy).

## 5. Tools
*   **Gymnasium:** For the environment interface.
*   **Stable-Baselines3:** For the PPO agent implementation.
*   **Pandas/Polars:** For high-performance time-series manipulation.

---

# Architecture Spec: Continuous Data Pipeline for RL Training

## 1. Project Context & Objective
We are transitioning the Gammarips system from a static signal generator to a dynamic Reinforcement Learning (RL) Training Environment.
*   **The Goal:** Train a Deep RL agent to actively manage options positions (Buy, Hold, Sell) by maximizing a Mark-to-Market Reward Function.
*   **The Constraint:** RL agents require sequential, unbroken time-series data to learn cause-and-effect. Our current "snapshot" approach (overwriting data daily) is insufficient. We must build a **Lifecycle Tracking Pipeline** that records the entire journey of a contract from "Signal Detection" to "Exit/Expiration."

## 2. The RL Data Requirement: "The Tape"
To implement a Mark-to-Market reward system ($R_t = V_t - V_{t-1} - \text{TimeDecay}$), the data pipeline must provide a "Tape"—a continuous historical log of market states.

### Logical Schema Requirements
The data structure must strictly separate **Observation Features** (what the agent sees) from **Execution Metrics** (what the environment uses to calculate PnL).

#### A. Observation Features (The Input State)
These are the inputs the neural network will consume to make decisions.
*   **Greeks:** Delta, Gamma, Theta, Vega (Normalized).
*   **Volatility State:** Implied Volatility (IV) Rank, Volume/Open Interest Ratio.
*   **Underlying Technicals:** RSI, ATR, and Moving Average distance relative to the **underlying asset** (e.g., SPY), not the option itself.

#### B. Execution Metrics (The Reward Drivers)
These are hidden from the agent during inference but are critical for the training environment to calculate rewards.
*   **Pricing:** Bid Price (Sell limit), Ask Price (Buy limit), Mid Price (Mark-to-market value).
*   **Timestamps:** Exact time of capture (essential for calculating time decay penalties).
*   **Liquidity:** Bid-Ask Spread width (used to simulate transaction costs/slippage).

## 3. Pipeline Logic: The "Sticky Watchlist"
The most significant architectural shift is moving from a **Scanner** mindset (stateless) to a **Tracker** mindset (stateful).

*   **Old Logic (Stateless):** Scan market. If criteria met -> Save. If criteria not met -> Ignore.
*   **New Logic (Stateful):** We must implement a **Persistence Layer (Active Watchlist)** that maintains "Sticky" focus on specific contracts.

### Scan Phase:
Identify new contracts exceeding our Gamma/Velocity thresholds. Add these unique Contract IDs to the Active Watchlist.

### Track Phase (The Heartbeat):
On every cron tick (e.g., 5 minutes), fetch data for **ALL** contracts currently in the Active Watchlist, regardless of whether they still meet the initial entry criteria.
*   *Why?* The RL agent needs to see the "exit" or "decay" phase of a trade, not just the entry. If we stop tracking a contract because it "cooled off," the agent never learns when to sell.

### Prune Phase:
Remove contracts from the Watchlist only when they reach a terminal state:
1.  **Expiration:** Contract is dead.
2.  **Stop Loss Limit:** Price dropped > 50% (simulating a bust).
3.  **Time Horizon:** Tracked for > 3 days (simulating our strategy maximum).

## 4. Synchronization Logic: Options vs. Underlying
A critical error in options data pipelines is "Desynchronization." The pipeline must ensure that the Option Data and the Underlying Stock Data are captured at the exact same moment.
*   **The Derivation Problem:** Calculating technical indicators (like RSI) on an option's price is noisy and statistically invalid due to wide spreads.
*   **The Solution:**
    1.  Fetch the Option Quote (Bid/Ask/Greeks).
    2.  Simultaneously fetch the Underlying Asset Quote (Price).
    3.  Compute Technical Indicators (RSI, ATR) on the Underlying Asset's recent price history.
    4.  **Join** these values into a single record before storage.
*   **Result:** The agent receives a feature vector that says: *"The Option Delta is 0.5 (Leverage), AND the Stock RSI is 80 (Overbought)."* This allows it to learn mean-reversion strategies.

## 5. Storage & Granularity
### Persistence Strategy
*   **Append-Only:** The database table must be an append-only log. Never UPDATE or TRUNCATE.
*   **Indexing:** High-performance indexing on `Contract_ID + Timestamp` is mandatory for efficient "Episode Replay" during training.

### Granularity (Sampling Rate)
*   **Target:** 5-Minute Intervals.
*   **Reasoning:** 1-minute data is too sparse (micro-noise). Daily data is too coarse (misses intraday volatility). 5-minute snapshots provide ~78 decision points per trading day, offering a perfect balance for Trajectory Optimization in PPO algorithms.

## 6. Summary of Action Items for Agent
1.  **Design the Schema:** Create a time-series optimized schema that supports the fields defined in Section 2.
2.  **Implement the Watchlist:** Build the logic to persist "Active" contract IDs across execution cycles.
3.  **Build the Joiner:** Implement the logic to calculate Underlying Technicals and attach them to Option rows in real-time.
4.  **Verify Continuity:** Ensure the pipeline handles market closes and gap-ups/downs correctly without breaking the time-series integrity.
