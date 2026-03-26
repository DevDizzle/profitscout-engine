import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# --- CURRENT TRACKING PARAMETERS (Regime-Aware V2) ---
# NOTE: These are based on a very small backtest sample (9 trades). 
# DO NOT USE THIS TO ALLOCATE REAL CAPITAL YET.
WIN_RATE = 0.556       # 55.6% hit the +40% target
STOP_RATE = 0.222      # 22.2% hit the -25% stop
TIMEOUT_RATE = 0.222   # 22.2% hit the 3-day timeout
TARGET_PCT = 0.40
STOP_PCT = -0.25
TIMEOUT_PCT = 0.00     # Assuming flat return on timeouts for conservatism

STARTING_CAPITAL = 2500
TRADES_PER_MONTH = 9   # Our strict filters drastically reduced trade volume
MONTHS_TO_SIMULATE = 12
SIMULATIONS = 10000

def run_simulation():
    print(f"Running Monte Carlo: {SIMULATIONS} lifetimes, {MONTHS_TO_SIMULATE} months...")
    print(f"Assumed Trade Dist: {WIN_RATE*100}% Win, {STOP_RATE*100}% Loss, {TIMEOUT_RATE*100}% Timeout")
    
    total_trades = TRADES_PER_MONTH * MONTHS_TO_SIMULATE
    results = []
    ruined_count = 0
    
    outcomes = [TARGET_PCT, STOP_PCT, TIMEOUT_PCT]
    probs = [WIN_RATE, STOP_RATE, TIMEOUT_RATE]

    for _ in range(SIMULATIONS):
        capital = STARTING_CAPITAL
        peak_capital = capital
        
        # Draw all trades for this lifetime at once
        draws = np.random.choice(outcomes, size=total_trades, p=probs)
        
        is_ruined = False
        for month in range(1, MONTHS_TO_SIMULATE + 1):
            # Scale bet sizes dynamically based on capital (e.g., risk 20% of account per trade)
            # or use fixed sizing. We'll use your old runway tier logic:
            if capital < 1500:
                bet_size = 500
            elif capital < 2500:
                bet_size = 1000
            elif capital < 4000:
                bet_size = 1500
            else:
                bet_size = 2000 # Max bet size
                
            # Process trades for the month
            for _ in range(TRADES_PER_MONTH):
                if capital <= bet_size:
                    bet_size = capital # Can't bet more than we have
                    
                if capital < 500:
                    is_ruined = True
                    break
                    
                trade_result = np.random.choice(outcomes, p=probs)
                capital += (bet_size * trade_result)
                
                if capital > peak_capital:
                    peak_capital = capital
            
            if is_ruined:
                break
                
            # Harvest logic: After month 3, if capital > $5000, withdraw $2000
            if month >= 4 and capital > 5000:
                capital -= 2000
                
        if is_ruined:
            ruined_count += 1
            results.append(0)
        else:
            results.append(capital)

    # Analytics
    results = np.array(results)
    risk_of_ruin = (ruined_count / SIMULATIONS) * 100
    median_ending = np.median(results)
    mean_ending = np.mean(results)
    
    print("\n" + "="*40)
    print("MONTE CARLO SIMULATION RESULTS (V2 REGIME)")
    print("="*40)
    print(f"Risk of Ruin (<$500): {risk_of_ruin:.2f}%")
    print(f"Median Ending Capital: ${median_ending:,.2f}")
    print(f"Average Ending Capital: ${mean_ending:,.2f}")
    print(f"Top 10% Outcome: ${np.percentile(results, 90):,.2f}")
    print("="*40)
    print("WARNING: This assumes the 9-trade holdout sample perfectly represents the future. It likely does not.")

if __name__ == "__main__":
    run_simulation()
