import random
import numpy as np

# Simulation parameters (Adjust these to test different scenarios)
WIN_RATE = 0.80
TRADES_PER_MONTH = 10
TAKE_PROFIT_PCT = 0.40  # +40%
STOP_LOSS_PCT = -0.25   # -25%
ALLOCATION_PER_TRADE = 750  # Average of $500 to $1000

NUM_SIMULATIONS = 10000

def run_simulation():
    total_profit = 0
    wins = 0
    losses = 0
    for _ in range(TRADES_PER_MONTH):
        if random.random() < WIN_RATE:
            total_profit += ALLOCATION_PER_TRADE * TAKE_PROFIT_PCT
            wins += 1
        else:
            total_profit += ALLOCATION_PER_TRADE * STOP_LOSS_PCT
            losses += 1
    return total_profit, wins, losses

results = [run_simulation()[0] for _ in range(NUM_SIMULATIONS)]

avg_profit = np.mean(results)
max_profit = np.max(results)
min_profit = np.min(results)
prob_positive = sum(1 for r in results if r > 0) / NUM_SIMULATIONS * 100

print(f"--- MONTE CARLO SIMULATION ({NUM_SIMULATIONS:,} runs) ---")
print(f"Parameters: {TRADES_PER_MONTH} trades, {WIN_RATE*100}% win rate, Target: +{TAKE_PROFIT_PCT*100}%, Stop: {STOP_LOSS_PCT*100}%")
print(f"Average Entry: ${ALLOCATION_PER_TRADE}")
print(f"Expected Average Monthly Profit: ${avg_profit:,.2f}")
print(f"Best Case Monthly Profit: ${max_profit:,.2f}")
print(f"Worst Case Monthly Profit: ${min_profit:,.2f}")
print(f"Probability of a Profitable Month: {prob_positive:.2f}%")
