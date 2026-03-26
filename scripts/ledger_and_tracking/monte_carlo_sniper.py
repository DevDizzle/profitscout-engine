import pandas as pd
import numpy as np
from google.cloud import bigquery

PROJECT_ID = "profitscout-fida8"

def run_monte_carlo():
    client = bigquery.Client(project=PROJECT_ID)
    
    # Fetch the exact cohort to get empirical returns
    query = f"""
    SELECT realized_return_pct
    FROM `{PROJECT_ID}.profit_scout.forward_paper_ledger`
    WHERE is_skipped = FALSE
      AND realized_return_pct IS NOT NULL
      AND VIX_at_entry <= 23.0
      AND premium_score >= 2
      AND NOT (VIX_at_entry >= 18.0 AND direction = 'BULLISH')
      AND NOT (VIX_at_entry < 18.0 AND direction = 'BEARISH')
    """
    
    try:
        df = client.query(query).to_dataframe()
    except Exception as e:
        print(f"Error querying BigQuery: {e}")
        return

    df['realized_return_pct'] = pd.to_numeric(df['realized_return_pct'], errors='coerce')
    returns_pool = df['realized_return_pct'].dropna().values
    
    print(f"Sampled {len(returns_pool)} historical trades for the Sniper Regime.")
    if len(returns_pool) == 0:
        print("No trades found in this cohort.")
        return

    # Backfill covered roughly 40 days (Feb 7 to Mar 19).
    historical_days = 40.0
    
    # Simulation parameters
    NUM_SIMULATIONS = 10000
    DAYS = 90
    EXPECTED_TRADES = (len(returns_pool) / historical_days) * DAYS 
    
    STARTING_CAPITAL = 1000.0
    TARGET_TRADE_SIZE = 750.0
    
    ending_capitals = []
    max_drawdowns = []
    
    np.random.seed(42) # For reproducibility
    
    for _ in range(NUM_SIMULATIONS):
        capital = STARTING_CAPITAL
        peak_capital = STARTING_CAPITAL
        max_dd = 0.0
        
        # Sample number of trades using Poisson distribution
        num_trades = np.random.poisson(EXPECTED_TRADES)
        
        for _ in range(num_trades):
            if capital <= 0:
                break
                
            # Determine trade size (up to 750, or whatever capital is left)
            trade_size = min(TARGET_TRADE_SIZE, capital)
            
            # Sample a return from the empirical distribution
            ret = np.random.choice(returns_pool)
            
            # Update capital
            profit_loss = trade_size * ret
            capital += profit_loss
            
            # Update peak and max drawdown
            if capital > peak_capital:
                peak_capital = capital
            
            dd = (peak_capital - capital) / peak_capital
            if dd > max_dd:
                max_dd = dd
                
        ending_capitals.append(capital)
        max_drawdowns.append(max_dd)
        
    # Analysis
    ending_capitals = np.array(ending_capitals)
    max_drawdowns = np.array(max_drawdowns)
    
    print(f"\n--- MONTE CARLO RESULTS (10,000 runs, {DAYS} days) ---")
    print(f"Starting Capital: ${STARTING_CAPITAL:,.2f}")
    print(f"Target Trade Size: ${TARGET_TRADE_SIZE:,.2f} (Fixed)")
    print(f"Expected Trades in 90 days: ~{EXPECTED_TRADES:.1f}")
    print("-" * 50)
    
    print(f"Average Ending Capital: ${np.mean(ending_capitals):,.2f}")
    print(f"Median Ending Capital: ${np.median(ending_capitals):,.2f}")
    print(f"P10 (Worst 10%) Ending Capital: ${np.percentile(ending_capitals, 10):,.2f}")
    print(f"P90 (Best 10%) Ending Capital: ${np.percentile(ending_capitals, 90):,.2f}")
    
    win_rate_sim = np.sum(ending_capitals > STARTING_CAPITAL) / NUM_SIMULATIONS
    print(f"\nProbability of Profit (> Initial Capital): {win_rate_sim:.2%}")
    
    # Risk of Ruin defined as dropping below $250 (unable to fund a meaningful trade)
    ruin_rate = np.sum(ending_capitals <= 250) / NUM_SIMULATIONS 
    print(f"Risk of Severe Depletion (< $250): {ruin_rate:.2%}")
    
    print(f"\nAverage Max Drawdown: {np.mean(max_drawdowns):.2%}")
    print(f"P90 Max Drawdown (Worst Case): {np.percentile(max_drawdowns, 90):.2%}")

if __name__ == "__main__":
    run_monte_carlo()
