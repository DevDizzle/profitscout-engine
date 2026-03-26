import pandas as pd
from google.cloud import bigquery

PROJECT_ID = "profitscout-fida8"

def run_liquidity_analysis():
    client = bigquery.Client(project=PROJECT_ID)

    query = """
    SELECT 
        ticker, scan_date, direction, recommended_contract, 
        recommended_volume, recommended_oi, recommended_spread_pct,
        recommended_mid_price, premium_score, is_win, peak_return_3d
    FROM `profitscout-fida8.profit_scout.overnight_signals_enriched`
    WHERE premium_score >= 2
      AND recommended_volume IS NOT NULL
      AND recommended_oi IS NOT NULL
      AND performance_updated IS NOT NULL
    """
    
    print("Fetching historical data for Premium Score >= 2...")
    df = client.query(query).to_dataframe()
    
    if len(df) == 0:
        print("No data found.")
        return

    print(f"Total Premium Score >= 2 signals: {len(df)}")
    
    df["scan_date"] = pd.to_datetime(df["scan_date"])
    days_span = (df["scan_date"].max() - df["scan_date"].min()).days
    months_span = max(1, days_span / 30.0)

    gates = {
        "Baseline (All Score >= 2)": lambda df: df,
        "V > 50 | OI > 100": lambda df: df[(df["recommended_volume"] > 50) | (df["recommended_oi"] > 100)],
        "V > 100 | OI > 250": lambda df: df[(df["recommended_volume"] > 100) | (df["recommended_oi"] > 250)],
        "V > 250 | OI > 500": lambda df: df[(df["recommended_volume"] > 250) | (df["recommended_oi"] > 500)],
        "V > 500 | OI > 1000": lambda df: df[(df["recommended_volume"] > 500) | (df["recommended_oi"] > 1000)],
        "V > 1000 | OI > 2000": lambda df: df[(df["recommended_volume"] > 1000) | (df["recommended_oi"] > 2000)],
        "V > 250 & OI > 250": lambda df: df[(df["recommended_volume"] > 250) & (df["recommended_oi"] > 250)],
        "V > 500 & OI > 500": lambda df: df[(df["recommended_volume"] > 500) & (df["recommended_oi"] > 500)],
        "V>100|OI>250 & Spread<=10%": lambda df: df[((df["recommended_volume"] > 100) | (df["recommended_oi"] > 250)) & (df["recommended_spread_pct"] <= 0.10)],
        "V>250|OI>500 & Spread<=10%": lambda df: df[((df["recommended_volume"] > 250) | (df["recommended_oi"] > 500)) & (df["recommended_spread_pct"] <= 0.10)],
        "V>500|OI>1000 & Spread<=10%": lambda df: df[((df["recommended_volume"] > 500) | (df["recommended_oi"] > 1000)) & (df["recommended_spread_pct"] <= 0.10)],
    }

    results = []
    
    for gate_name, gate_func in gates.items():
        gated_df = gate_func(df)
        pass_count = len(gated_df)
        if pass_count == 0: 
            continue
            
        win_rate = gated_df['is_win'].sum() / pass_count
        avg_peak = gated_df['peak_return_3d'].mean()
        monthly_trades = pass_count / months_span
        
        results.append({
            "Gate": gate_name,
            "Trades": pass_count,
            "Trades/Month": round(monthly_trades, 1),
            "Win Rate": f"{win_rate*100:.1f}%",
            "Avg Peak Return (Underlying)": round(avg_peak, 2)
        })

    res_df = pd.DataFrame(results).sort_values(by="Avg Peak Return (Underlying)", ascending=False)
    
    print("\n=== LIQUIDITY GATES BACKTEST (PREMIUM SCORE >= 2) ===")
    print(res_df.to_string(index=False))

if __name__ == "__main__":
    run_liquidity_analysis()
