import os
import pandas as pd
import yfinance as yf
from google.cloud import bigquery
from datetime import datetime, timedelta, timezone

PROJECT_ID = "profitscout-fida8"
DATASET = "profit_scout"
TABLE_NAME = "overnight_signals_enriched"

def get_signals_to_backfill(client):
    query = f"""
    SELECT ticker, scan_date, direction, underlying_price
    FROM `{PROJECT_ID}.{DATASET}.{TABLE_NAME}`
    WHERE performance_updated IS NULL
      AND scan_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    """
    job = client.query(query)
    return [dict(row) for row in job.result()]

def process_signals():
    client = bigquery.Client(project=PROJECT_ID)
    signals = get_signals_to_backfill(client)
    
    if not signals:
        print("No signals to backfill.")
        return

    print(f"Found {len(signals)} signals to backfill.")
    
    tickers = list(set([s['ticker'] for s in signals]))
    min_date = min([s['scan_date'] for s in signals])
    max_date = max([s['scan_date'] for s in signals])
    
    start_date = min_date.strftime("%Y-%m-%d")
    end_date = (max_date + timedelta(days=10)).strftime("%Y-%m-%d")
    
    print(f"Fetching data for {len(tickers)} tickers from {start_date} to {end_date}")
    
    try:
        # group_by='ticker' behaves slightly differently, omitting it puts Price attrs as the top level
        # let's download default, which puts Price Attr at level 0 and Ticker at level 1
        data = yf.download(tickers, start=start_date, end=end_date)
    except Exception as e:
        print(f"Failed to fetch data from yfinance: {e}")
        return

    updates_dict = {}
    
    for s in signals:
        ticker = s['ticker']
        scan_date = s['scan_date']
        direction = s['direction']
        underlying_price = s['underlying_price']
        
        if pd.isna(underlying_price) or underlying_price <= 0:
            print(f"Invalid underlying price for {ticker} on {scan_date}")
            continue

        try:
            if len(tickers) == 1:
                ticker_data = data
            else:
                if ticker not in data.columns.levels[1]:
                    print(f"Ticker {ticker} not found in yfinance data.")
                    continue
                ticker_data = data.xs(ticker, axis=1, level=1)
                
            ticker_data = ticker_data.dropna(subset=['Close'])
            future_data = ticker_data[ticker_data.index.date > scan_date]
            
            if len(future_data) < 3:
                print(f"Not enough trading days after {scan_date} for {ticker}")
                continue
                
            t1, t2, t3 = future_data.iloc[0], future_data.iloc[1], future_data.iloc[2]
            
            t1_close = t1['Close']
            t2_close = t2['Close']
            t3_close = t3['Close']
            
            t1_high, t2_high, t3_high = t1['High'], t2['High'], t3['High']
            t1_low, t2_low, t3_low = t1['Low'], t2['Low'], t3['Low']
            
            next_day_pct = ((t1_close - underlying_price) / underlying_price) * 100
            day2_pct = ((t2_close - underlying_price) / underlying_price) * 100
            day3_pct = ((t3_close - underlying_price) / underlying_price) * 100
            
            if direction == "BULLISH":
                peak_return_3d = ((max(t1_high, t2_high, t3_high) - underlying_price) / underlying_price) * 100
            else:
                peak_return_3d = ((underlying_price - min(t1_low, t2_low, t3_low)) / underlying_price) * 100
                
            if peak_return_3d >= 5.0:
                outcome_tier = "home_run"
            elif peak_return_3d >= 3.0:
                outcome_tier = "strong"
            elif peak_return_3d >= 1.0:
                outcome_tier = "directional"
            elif peak_return_3d >= 0.0:
                outcome_tier = "flat"
            else:
                outcome_tier = "wrong"
                
            is_win = bool(peak_return_3d >= 1.0)
            
            updates_dict[(ticker, scan_date.strftime("%Y-%m-%d"))] = {
                'ticker': ticker,
                'scan_date': scan_date.strftime("%Y-%m-%d"),
                'next_day_close': float(t1_close),
                'next_day_pct': float(next_day_pct),
                'day2_close': float(t2_close),
                'day2_pct': float(day2_pct),
                'day3_close': float(t3_close),
                'day3_pct': float(day3_pct),
                'peak_return_3d': float(peak_return_3d),
                'outcome_tier': outcome_tier,
                'is_win': is_win,
                'performance_updated': datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            }
        except Exception as e:
            print(f"Error processing {ticker} on {scan_date}: {e}")
            continue

    updates = list(updates_dict.values())
    if not updates:
        print("No valid updates to process.")
        return

    print(f"Writing {len(updates)} updates back to BigQuery using MERGE...")
    
    temp_table_id = f"{PROJECT_ID}.{DATASET}.temp_perf_updates"
    
    schema = [
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("scan_date", "DATE"),
        bigquery.SchemaField("next_day_close", "FLOAT"),
        bigquery.SchemaField("next_day_pct", "FLOAT"),
        bigquery.SchemaField("day2_close", "FLOAT"),
        bigquery.SchemaField("day2_pct", "FLOAT"),
        bigquery.SchemaField("day3_close", "FLOAT"),
        bigquery.SchemaField("day3_pct", "FLOAT"),
        bigquery.SchemaField("peak_return_3d", "FLOAT"),
        bigquery.SchemaField("outcome_tier", "STRING"),
        bigquery.SchemaField("is_win", "BOOLEAN"),
        bigquery.SchemaField("performance_updated", "TIMESTAMP"),
    ]
    
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    
    try:
        load_job = client.load_table_from_json(updates, temp_table_id, job_config=job_config)
        load_job.result()
        
        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET}.{TABLE_NAME}` T
        USING `{temp_table_id}` S
        ON T.ticker = S.ticker AND T.scan_date = S.scan_date
        WHEN MATCHED THEN
          UPDATE SET 
            next_day_close = S.next_day_close,
            next_day_pct = S.next_day_pct,
            day2_close = S.day2_close,
            day2_pct = S.day2_pct,
            day3_close = S.day3_close,
            day3_pct = S.day3_pct,
            peak_return_3d = S.peak_return_3d,
            outcome_tier = S.outcome_tier,
            is_win = S.is_win,
            performance_updated = S.performance_updated
        """
        merge_job = client.query(merge_query)
        merge_job.result()
        print(f"Successfully merged {len(updates)} rows.")
        
    except Exception as e:
        print(f"Error updating BigQuery: {e}")

if __name__ == "__main__":
    process_signals()
