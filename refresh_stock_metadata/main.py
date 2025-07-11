import os
import time
import logging
import requests
import pandas as pd
import json
from datetime import date
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery, storage, pubsub_v1
import google.cloud.logging
from tenacity import retry, stop_after_attempt, wait_exponential
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
DATASET_ID = os.getenv("BQ_DATASET", "profit_scout")
TABLE_ID = os.getenv("MASTER_TABLE", "stock_metadata")
TICKER_TXT_GCS = os.getenv("TICKER_TXT_GCS", "gs://profit-scout-data/tickerlist.txt")
FMP_KEY = os.environ.get("FMP_API_KEY")
PUB_SUB_TOPIC = "new-metadata-found"
MAX_RETRIES = 3
REQUEST_DELAY = 0.03
MAX_WORKERS = 4

# --- Initialize Clients ---
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()

bq_client = bigquery.Client(project=PROJECT_ID)
gcs_client = storage.Client(project=PROJECT_ID)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUB_SUB_TOPIC)


@retry(stop=stop_after_attempt(MAX_RETRIES),
       wait=wait_exponential(multiplier=1, min=2, max=10))
def get_json(url: str):
    """Fetch JSON data from an HTTP endpoint with retry logic."""
    if not FMP_KEY:
        raise ValueError("FMP_API_KEY environment variable is not set.")
    
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.json()

def get_last_n_quarters(n: int = 12) -> list[tuple[int, int]]:
    """Generates a list of the last n (year, quarter) tuples."""
    today = date.today()
    quarters = []
    current_date = today
    for _ in range(n):
        current_date = current_date - relativedelta(months=3)
        quarter = (current_date.month - 1) // 3 + 1
        quarters.append((current_date.year, quarter))
    return quarters

def load_tickers_from_gcs(gcs_path: str) -> list[str]:
    """Loads a list of tickers from a text file in GCS."""
    try:
        bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            logging.error(f"Ticker file not found at {gcs_path}")
            return []
        ticker_data = blob.download_as_text(encoding="utf-8")
        tickers = [line.strip().upper() for line in ticker_data.strip().split("\n") if line.strip()]
        logging.info(f"Loaded {len(tickers)} tickers from {gcs_path}")
        return tickers
    except Exception as e:
        logging.error(f"Failed to load tickers from GCS: {e}", exc_info=True)
        return []

def fetch_profiles_bulk(symbols: list[str]) -> pd.DataFrame:
    """Retrieve company profiles for a list of tickers in batches."""
    all_profiles = []
    for i in range(0, len(symbols), 200):
        batch = symbols[i:i + 200]
        url = f"https://financialmodelingprep.com/api/v3/profile/{','.join(batch)}?apikey={FMP_KEY}"
        try:
            data = get_json(url)
            if isinstance(data, list) and data:
                df = pd.DataFrame(data)
                df = df[["symbol", "companyName", "industry", "sector"]].rename(
                    columns={"symbol": "ticker", "companyName": "company_name"}
                )
                all_profiles.append(df)
        except Exception as e:
            logging.warning(f"Profile fetch failed for batch starting with {batch[0]}: {e}")
        time.sleep(REQUEST_DELAY)
    if not all_profiles:
        return pd.DataFrame()
    return pd.concat(all_profiles, ignore_index=True)


def process_ticker(ticker: str, profile_info: dict, quarters_to_check: list) -> list[dict]:
    """
    Contains all the logic to fetch and process data for one ticker.
    This function will be executed in a separate thread for each ticker.
    """
    try:
        statements_url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&limit=24&apikey={FMP_KEY}"
        statements_data = get_json(statements_url)
        statements_map = {}
        if isinstance(statements_data, list):
            for stmt in statements_data:
                try:
                    year = int(stmt['calendarYear'])
                    quarter_num = int(stmt['period'].replace('Q', ''))
                    statements_map[(year, quarter_num)] = stmt.get('date')
                except (ValueError, TypeError, KeyError):
                    continue
        
        if not statements_map:
            logging.warning(f"Could not build statement map for {ticker}. Skipping.")
            return []

        ticker_records = []
        for year, quarter in quarters_to_check:
            try:
                transcript_url = f"https://financialmodelingprep.com/api/v3/earning_call_transcript/{ticker}?year={year}&quarter={quarter}&apikey={FMP_KEY}"
                transcript_data = get_json(transcript_url)
                if transcript_data and isinstance(transcript_data, list):
                    transcript = transcript_data[0]
                    earnings_call_date = transcript.get('date')
                    quarter_end_date = statements_map.get((year, quarter))
                    if earnings_call_date and quarter_end_date:
                        # --- CHANGE 1: Add the new fields to the record ---
                        record = {
                            "ticker": ticker,
                            "company_name": profile_info["company_name"],
                            "industry": profile_info["industry"],
                            "sector": profile_info["sector"],
                            "quarter_end_date": quarter_end_date,
                            "earnings_call_date": earnings_call_date,
                            "earnings_year": year,
                            "earnings_quarter": quarter,
                        }
                        ticker_records.append(record)
                time.sleep(REQUEST_DELAY)
            except Exception:
                continue
        
        logging.info(f"Processed {ticker}, found {len(ticker_records)} records.")
        return ticker_records

    except Exception as e:
        logging.error(f"Failed to process ticker {ticker}: {e}", exc_info=True)
        return []


def refresh_stock_metadata(request):
    """
    HTTP Cloud Function to refresh the stock metadata table by fetching data
    from FMP API and merging it into a BigQuery table.
    """
    tickers = load_tickers_from_gcs(TICKER_TXT_GCS)
    if not tickers:
        logging.critical("Ticker list is empty. Aborting run.")
        return "Ticker list load failure", 500

    profiles_df = fetch_profiles_bulk(tickers)
    if profiles_df.empty:
        logging.critical("Profile fetch failed for all tickers. Aborting run.")
        return "Profile fetch failed", 500

    all_records = []
    quarters_to_check = get_last_n_quarters(12)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {
            executor.submit(process_ticker, ticker, profiles_df.loc[profiles_df["ticker"] == ticker].iloc[0].to_dict(), quarters_to_check): ticker
            for ticker in tickers if not profiles_df.loc[profiles_df["ticker"] == ticker].empty
        }

        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                records = future.result()
                if records:
                    all_records.extend(records)
            except Exception as e:
                logging.error(f"Ticker {ticker} generated an exception: {e}", exc_info=True)
    
    logging.info(f"Collected a total of {len(all_records)} records from all tickers.")

    if not all_records:
        logging.error("No data was processed for any ticker.")
        return "No data to process", 400

    final_df = pd.DataFrame(all_records)
    final_df["quarter_end_date"] = pd.to_datetime(final_df["quarter_end_date"], errors='coerce').dt.date
    final_df["earnings_call_date"] = pd.to_datetime(final_df["earnings_call_date"], errors='coerce').dt.date
    
    final_df = final_df.where(pd.notnull(final_df), None)
    final_df.dropna(subset=['ticker', 'quarter_end_date', 'earnings_call_date'], inplace=True)
    final_df.drop_duplicates(subset=['ticker', 'quarter_end_date'], inplace=True, keep='first')
    
    # --- CHANGE 2: Update schema to include new columns ---
    schema = [
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("industry", "STRING"),
        bigquery.SchemaField("sector", "STRING"),
        bigquery.SchemaField("quarter_end_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("earnings_call_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("earnings_year", "INTEGER"),
        bigquery.SchemaField("earnings_quarter", "INTEGER"),
    ]
    
    temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}_temp_{int(time.time())}"
    target_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
    bq_client.load_table_from_dataframe(final_df, temp_table_id, job_config=job_config).result()
    logging.info(f"Loaded {len(final_df)} records into temporary table {temp_table_id}")

    try:
        bq_client.get_table(target_table_id)
    except Exception:
        logging.info(f"Target table {target_table_id} not found. Creating it now.")
        table = bigquery.Table(target_table_id, schema=schema)
        bq_client.create_table(table)
    
    # --- CHANGE 3: Update MERGE statement for new columns ---
    merge_sql = f"""
    MERGE `{target_table_id}` T
    USING `{temp_table_id}` S
      ON T.ticker = S.ticker AND T.quarter_end_date = S.quarter_end_date
    WHEN MATCHED THEN
      UPDATE SET 
        earnings_call_date = S.earnings_call_date,
        earnings_year = S.earnings_year,
        earnings_quarter = S.earnings_quarter
    WHEN NOT MATCHED THEN
      INSERT (ticker, company_name, industry, sector, quarter_end_date, earnings_call_date, earnings_year, earnings_quarter)
      VALUES (S.ticker, S.company_name, S.industry, S.sector, S.quarter_end_date, S.earnings_call_date, S.earnings_year, S.earnings_quarter)
    """
    bq_client.query(merge_sql).result()
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    logging.info(f"Successfully merged data into {target_table_id}")

    try:
        message_data = {"status": "complete", "service": "refresh_stock_metadata"}
        future = publisher.publish(topic_path, json.dumps(message_data).encode("utf-8"))
        logging.info(f"Published completion message to {PUB_SUB_TOPIC} with ID: {future.result()}")
    except Exception as e:
        logging.error(f"Failed to publish to Pub/Sub: {e}", exc_info=True)
        return f"Process completed but failed to publish to Pub/Sub: {e}", 500

    return f"Successfully refreshed {target_table_id} and published completion message.", 200