# devdizzle/profitscout-data-pipeline/profitscout-data-pipeline-master/refresh_stock_metadata/main.py
import os
import time
import logging
import requests
import pandas as pd
import json
from google.cloud import bigquery, storage, pubsub_v1
from tenacity import retry, stop_after_attempt, wait_exponential

# Runtime configuration
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
DATASET    = os.getenv("BQ_DATASET", "profit_scout")
TABLE      = os.getenv("MASTER_TABLE", "stock_metadata")
TICKER_TXT_GCS = os.getenv("TICKER_TXT_GCS", "gs://profit-scout-data/tickerlist.txt")
FMP_KEY    = os.environ["FMP_API_KEY"]
PUB_SUB_TOPIC = "new-metadata-found"
MAX_RETRIES  = 3
REQUEST_DELAY = 0.02

# Initialize clients
bq  = bigquery.Client(project=PROJECT_ID)
gcs = storage.Client(project=PROJECT_ID)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUB_SUB_TOPIC)


logging.basicConfig(level=logging.INFO,
                      format="%(asctime)s - %(levelname)s - %(message)s")

@retry(stop=stop_after_attempt(MAX_RETRIES),
       wait=wait_exponential(multiplier=1, min=2, max=10))
def get_json(url: str):
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()

def load_tickers_from_txt(path: str) -> list[str]:
    """Loads a list of tickers from a .txt file in GCS."""
    bucket_name, blob_name = path.replace("gs://", "").split("/", 1)
    bucket = gcs.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    if not blob.exists():
        logging.error(f"Ticker file not found at {path}")
        return []

    ticker_data = blob.download_as_text(encoding="utf-8")
    tickers = [
        line.strip().upper()
        for line in ticker_data.strip().split("\n")
        if line.strip()
    ]
    logging.info("Loaded %d tickers from .txt file", len(tickers))
    return tickers

def fetch_profiles_bulk(symbols: list[str]) -> pd.DataFrame:
    frames = []
    for i in range(0, len(symbols), 100):
        batch = symbols[i:i + 100]
        url = (
            f"https://financialmodelingprep.com/api/v3/profile/"
            f"{','.join(batch)}?apikey={FMP_KEY}"
        )
        try:
            data = get_json(url)
            if data:
                frames.append(
                    pd.DataFrame(data)[["symbol", "companyName", "industry", "sector"]]
                    .rename(columns={"symbol": "ticker", "companyName": "company_name"})
                )
        except Exception as e:
            logging.warning("Profile fetch failed for %s..: %s", batch[0], e)
        time.sleep(REQUEST_DELAY)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def fetch_quarterly_ends(ticker: str) -> pd.DataFrame:
    url = (
        f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}"
        f"?period=quarter&limit=12&apikey={FMP_KEY}"
    )
    try:
        data = get_json(url)
        return pd.DataFrame(data)[["date"]].rename(columns={"date": "quarter_end_date"}) if data else pd.DataFrame()
    except Exception as e:
        logging.warning("Quarterly ends fetch failed for %s: %s", ticker, e)
        return pd.DataFrame()

def fetch_earnings_calls(ticker: str) -> pd.DataFrame:
    url = (
        f"https://financialmodelingprep.com/api/v3/historical/earning_calendar/{ticker}"
        f"?limit=12&apikey={FMP_KEY}"
    )
    try:
        data = get_json(url)
        return pd.DataFrame(data)[["date"]].rename(columns={"date": "earnings_call_date"}) if data else pd.DataFrame()
    except Exception as e:
        logging.warning("Earnings call fetch failed for %s: %s", ticker, e)
        return pd.DataFrame()

def refresh_stock_metadata(request):
    tickers = load_tickers_from_txt(TICKER_TXT_GCS)
    if not tickers:
        return "Ticker list load failure", 500

    profiles = fetch_profiles_bulk(tickers)
    if profiles.empty:
        return "Profile fetch failed", 500

    rows = []
    for tkr in tickers:
        profile = profiles.loc[profiles["ticker"] == tkr]
        if profile.empty:
            continue
        qs = fetch_quarterly_ends(tkr)
        if qs.empty:
            continue
        ecs = fetch_earnings_calls(tkr)
        if ecs.empty:
            ecs = pd.DataFrame({"earnings_call_date": [None] * len(qs)})

        n = min(len(qs), len(ecs))
        merged = pd.concat([qs.head(n), ecs.head(n)], axis=1)
        merged["ticker"]        = tkr
        merged["company_name"]  = profile.iloc[0]["company_name"]
        merged["industry"]      = profile.iloc[0]["industry"]
        merged["sector"]        = profile.iloc[0]["sector"]
        rows.append(merged)
        time.sleep(REQUEST_DELAY)

    if not rows:
        return "No data to process", 400

    quarters = pd.concat(rows, ignore_index=True)
    quarters["quarter_end_date"]  = pd.to_datetime(quarters["quarter_end_date"]).dt.date
    quarters["earnings_call_date"] = pd.to_datetime(quarters["earnings_call_date"]).dt.date

    df = quarters[[
        "ticker", "company_name", "industry", "sector", "quarter_end_date", "earnings_call_date"
    ]].where(pd.notnull(quarters), None)

    schema = [
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("industry", "STRING"),
        bigquery.SchemaField("sector", "STRING"),
        bigquery.SchemaField("quarter_end_date", "DATE"),
        bigquery.SchemaField("earnings_call_date", "DATE"),
    ]

    temp_table = f"{PROJECT_ID}.{DATASET}.{TABLE}_temp"
    bq.load_table_from_dataframe(
        df,
        temp_table,
        bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema),
    ).result()

    target_table = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    try:
        bq.get_table(target_table)
    except Exception:
        bq.create_table(bigquery.Table(target_table, schema=schema))

    merge_sql = f"""
    MERGE `{target_table}` T
    USING `{temp_table}` S
      ON T.ticker = S.ticker AND T.quarter_end_date = S.quarter_end_date
    WHEN MATCHED THEN UPDATE SET
        company_name       = S.company_name,
        industry           = S.industry,
        sector             = S.sector,
        earnings_call_date = S.earnings_call_date
    WHEN NOT MATCHED THEN
        INSERT (ticker, company_name, industry, sector, quarter_end_date, earnings_call_date)
        VALUES (S.ticker, S.company_name, S.industry, S.sector, S.quarter_end_date, S.earnings_call_date)
    """
    bq.query(merge_sql).result()
    bq.delete_table(temp_table, not_found_ok=True)
    logging.info("Refresh complete: %d tickers", len(tickers))

    # --- Publish completion message to Pub/Sub ---
    try:
        message_data = {"status": "complete", "service": "refresh_stock_metadata"}
        future = publisher.publish(topic_path, json.dumps(message_data).encode("utf-8"))
        logging.info(f"Published completion message to {PUB_SUB_TOPIC} with ID: {future.result()}")
    except Exception as e:
        logging.error(f"Failed to publish to Pub/Sub topic {PUB_SUB_TOPIC}: {e}")
        # Decide if this should be a critical error
        return f"Process completed but failed to publish to Pub/Sub: {e}", 500


    return f"Successfully refreshed {target_table} and published completion message.", 200