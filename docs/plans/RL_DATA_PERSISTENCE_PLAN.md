# RL Data Persistence Plan: "Sidecar Archiver"

This approach respects your constraints: we will not modify the existing `TRUNCATE` logic or the `options_chain` table. Instead, we will implement a "Sidecar Archiver" pattern.

## The Plan: "Snapshot & Archive"

We will create a parallel history layer that passively captures the daily state without interfering with the live pipeline.

### 1. Infrastructure (New Tables)
We have created two new partitioned BigQuery tables to serve as the RL Dataset Source:
*   `options_chain_history`: Captures the daily "Market State" (Greeks, IV, Price) for every contract.
*   `technicals_history`: Captures the daily "Stock State" (RSI, MACD, SMA status) for every ticker.

### 2. Pipeline Additions (Low Touch)

#### A. The Options Archiver (New Script)
*   **File:** `src/ingestion/core/pipelines/history_archiver.py`
*   **Logic:** Runs immediately after `options_chain_fetcher`.
*   **Action:** Executes a simple SQL `INSERT` query:
    ```sql
    INSERT INTO `{PROJECT}.{DATASET}.options_chain_history`
    SELECT *, CURRENT_DATE() as snapshot_date
    FROM `{PROJECT}.{DATASET}.options_chain`
    ```
*   **Benefit:** Preserves the daily data before the next day's TRUNCATE wipes it.

#### B. The Technicals Persister (Minor Edit)
*   **Target:** `src/ingestion/core/pipelines/technicals_collector.py`
*   **Logic:** This script now calculates indicators and saves the `update_rows` (Latest RSI, MACD, etc.) to `technicals_history`.
*   **Benefit:** Ensures we have the "Environment Features" (Technicals) aligned with the "Action Features" (Options) for every single day.

### 3. RL Dataset Creation (Future Async Job)
Once these two tables are populating daily, we can create the training set anytime by joining them:
*   `RL_State` = `options_chain_history` + `technicals_history` (Joined on Ticker + Date).
*   `RL_Reward` = Query `price_data` for T+3 days to calculate the outcome.

### Status Update

**Completed Steps (within this session):**
1.  Created `options_chain_history` and `technicals_history` BigQuery tables.
2.  Created `src/ingestion/core/pipelines/history_archiver.py`.
3.  Modified `src/ingestion/core/pipelines/technicals_collector.py` to write to `technicals_history`.
4.  Integrated the `history_archiver` into `src/ingestion/main.py` to run after `options_chain_fetcher`.

**Next Steps (remaining for the full RL project as per `RL_STRATEGY_REPORT.md`):**

*   **Phase 1: Dataset Construction:** Now that the historical data is being collected, the next step is to build the comprehensive RL dataset by joining `options_chain_history` and `technicals_history`. This will involve a script to extract, transform, and load this combined data for RL training.
*   **Phase 2: Model Selection & Training:**
    *   Implement a Supervised Learning baseline to validate the feature set and establish a performance benchmark.
    *   Develop and train an RL agent (e.g., using PPO or DQN) on the constructed dataset.
*   **Phase 3: Deployment & Evaluation:** Deploy the trained RL model into a "paper trading" loop to filter recommendations and continuously evaluate its performance against the desired criteria (>50% gain within 3 days).