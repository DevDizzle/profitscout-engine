# Protocol-Accurate Intraday Execution Backtest Plan

## Objective
Convert abstract signal research (peak return within 3 days) into a protocol-accurate intraday execution backtest. We need to determine the **realized expectancy (EV)** and **realized win rate** of the two target strategies (`HEDGE + HIGH_RR` and `HEDGE + HIGH_ATR`) under strict live-trading constraints.

## System Parameters & Rules
*   **Target Signals:** Only signals flagged as `is_tradeable = TRUE` from the `overnight_signals_enriched` BigQuery table.
*   **Entry:** Next valid trading session at exactly **9:45 AM ET**.
*   **Exit Target:** **+40%** gain from the scenario-specific entry price.
*   **Exit Stop:** **-25%** loss from the scenario-specific entry price.
*   **Timeout:** If neither target nor stop is hit by the final regular-hours minute (15:59 ET) of the 3rd trading session, exit at that bar's closing price.
*   **Execution Assumption:** Mechanical, no emotion, sequential capital deployment.

## Modeling Challenges & Solutions
1.  **Mid-Price Ambiguity:** 9:45 AM `close` is not guaranteed to be the NBBO midpoint. We will treat it as a "trade-based executable proxy" and run sensitivity models (Optimistic, Base Case, Stress Case) with varying slippage penalties.
2.  **Scenario-Specific Thresholds:** Because entry prices vary by scenario (due to slippage), the +40% target and -25% stop prices must be calculated dynamically off the penalized entry price for each scenario.
3.  **Intrabar Ambiguity:** If both target and stop are touched in the same 1-minute candle, we strictly assume the **STOP (-25%)** was hit first.
4.  **Liquidity Risk:** If the 9:45 AM bar does not exist or has `volume == 0`, the trade is skipped and flagged as `invalid_liquidity`.
5.  **Trading Calendar:** "3 days" strictly means 3 trading sessions. The script must use an exchange calendar (e.g., via `pandas_market_calendars` or `yfinance` history) to calculate `entry_day` and `timeout_day`, accounting for holidays and weekends.

## The Three Backtest Scenarios
To ensure the system survives contact with reality, the script will simulate three parallel realities.

### A. Optimistic Case
*   **Entry:** Raw 9:45 AM `close` price exactly.
*   **Target/Stop:** Computed off raw entry.
*   **Exit Fills:** Fixed threshold execution (exactly +40% or -25% return).
*   **Rule:** If both hit in same minute -> Stop first.

### B. Base Case (The Realistic Benchmark)
*   **Entry:** 9:45 AM `close` price **+ 2% slippage penalty** (pay 2% more to enter).
*   **Target/Stop:** Computed off the +2% penalized entry.
*   **Exit Fills:** Fixed threshold execution (exactly +40% or -25% return based on the *base* entry).
*   **Rule:** If both hit in same minute -> Stop first.

### C. Stress Case (The System Breaker)
*   **Entry:** 9:45 AM `close` price **+ 5% slippage penalty** (pay 5% more to enter).
*   **Target/Stop:** Computed off the +5% penalized entry.
*   **Exit Fills:** Adverse fill realism model.
    *   **Stop Fill:** If breached, exit at the *worse* of the stop price OR the minute's `close` price.
    *   **Timeout Fill:** Day 3 final `close` **- 5% slippage penalty**.
*   **Rule:** If both hit in same minute -> Stop first.

## Data Source & API
*   **API:** Polygon.io Options Aggregates (Bars) API.
*   **Endpoint:** `v2/aggs/ticker/{options_ticker}/range/1/minute/{from}/{to}?adjusted=true&sort=asc&apiKey={POLYGON_API_KEY}`
*   **Format:** Options tickers must be constructed exactly as `O:[Underlying][Expiry Date YYMMDD][Type C/P][Strike Price 8-digits]`.

## Required Deliverables (The Output)

The simulation script must produce two outputs. The ledger will be in **long format** (one row per trade per scenario) to make downstream aggregation clean.

### 1. Per-Trade Ledger (`trade_ledger.csv`)
For every signal evaluated across every scenario, record:
*   `ticker`
*   `scan_date`
*   `entry_session_date` (The actual date of the 9:45 entry)
*   `timeout_session_date` (The end of day 3)
*   `combo_type` (HEDGE_HIGH_RR or HEDGE_HIGH_ATR)
*   `direction`
*   `options_ticker` (e.g., O:SPY230519C0400000)
*   `dte_at_entry`
*   `scenario` (Optimistic, Base, Stress)
*   `liquidity_flag` (Valid, Invalid)
*   `entry_timestamp`
*   `entry_bar_volume`
*   `entry_price` (Scenario-specific)
*   `target_price` (Scenario-specific)
*   `stop_price` (Scenario-specific)
*   `exit_timestamp`
*   `exit_reason` (`target`, `stop`, `timeout`, `invalid_liquidity`)
*   `bars_available` (Total minute bars fetched for the 3-day window)
*   `bars_to_exit` (Holding time in minutes)
*   `exit_price` (Scenario-specific execution price)
*   `return_pct` (Calculated as `(exit_price - entry_price) / entry_price`)

### 2. Strategy Aggregation Report (`system_report.txt` / stdout)
A summary computing the following metrics aggregated by `scenario` and `combo_type`:
*   Total Valid Trades
*   Realized Win Rate (Hits Target)
*   Loss Rate (Hits Stop)
*   Timeout Rate (Hits Day 3 Close)
*   Expected Value (EV) per trade
*   Average Return
*   Median Return
*   Max Losing Streak
*   Estimated Monthly Trade Count (Total valid trades / Number of months in dataset)

## Step-by-Step Execution Plan

1.  **Step 1: Data Extraction**
    *   Query BigQuery explicitly: `SELECT ticker, scan_date, recommended_contract, recommended_strike, recommended_expiration, direction, premium_hedge, premium_high_rr, premium_high_atr FROM profitscout-fida8.profit_scout.overnight_signals_enriched WHERE is_tradeable = TRUE`.
    *   Derive `combo_type` from the premium flags.

2.  **Step 2: Polygon Ticker Construction**
    *   Build a helper function to convert `recommended_contract` into the strict 21-character Polygon options ticker format (e.g., `O:SPY241220C00500000`).

3.  **Step 3: Market Calendar & Window Definition**
    *   Use a market calendar utility to find the next valid trading day (`entry_day`) following `scan_date`.
    *   Calculate the 3rd trading day from `entry_day` to define the `timeout_day`.

4.  **Step 4: Intraday Data Fetching**
    *   Call the Polygon API to fetch 1-minute bars for that specific options contract from `entry_day` 09:30 AM ET through `timeout_day` 16:00 PM ET. Handle rate limits.

5.  **Step 5: The 9:45 AM Entry Filter**
    *   Scan the fetched bars for the 09:45 AM ET bar on `entry_day`.
    *   If missing or `volume == 0`, mark `liquidity_flag = Invalid` and create 3 dummy rows in the ledger (one for each scenario). Skip simulation loop for this trade.

6.  **Step 6: The Simulation Loop (Run 3x per Trade)**
    *   For the Optimistic, Base, and Stress scenarios:
        *   Calculate the scenario-specific `entry_price`.
        *   Calculate the scenario-specific `target_price` and `stop_price`.
        *   Iterate chronologically through the remaining 1-minute bars.
        *   Apply the scenario's exit logic (fixed vs. adverse realism) based on intrabar `high` and `low`.
        *   Record the result in the long-format ledger.

7.  **Step 7: Analysis & Output**
    *   Convert the ledger to a pandas DataFrame.
    *   Group by `scenario` and `combo_type` to calculate the aggregate metrics (Win Rate, EV, Max Losing Streak, etc.).
    *   Print the system report to stdout.
    *   Save `trade_ledger.csv` to disk.
