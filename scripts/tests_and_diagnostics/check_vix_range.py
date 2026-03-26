import yfinance as yf
import pandas as pd

# Fetch VIX data for the period we have database records for
vix_data = yf.download("^VIX", start="2026-02-21", end="2026-03-14", progress=False)
vix_data.reset_index(inplace=True)
print("VIX Range during our recorded data (Feb 21 - Mar 13):")
print(f"Min VIX: {vix_data.Close.min().iloc[0]:.2f}")
print(f"Max VIX: {vix_data.Close.max().iloc[0]:.2f}")
print(f"Mean VIX: {vix_data.Close.mean().iloc[0]:.2f}")

