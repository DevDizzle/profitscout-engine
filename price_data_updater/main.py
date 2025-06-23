import datetime
import logging
import os
import pandas as pd
import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from threading import Lock
import json
import time
import concurrent.futures
import base64
from google.cloud import pubsub_v1   

# ---------- CONFIGURATION ----------
FMP_API_KEY = os.getenv("FMP_API_KEY")
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "price_data")
TICKER_LIST_PATH = "tickerlist.xlsx"
LOOKBACK_DAYS = 7
DEFAULT_START_DATE = datetime.date(2022, 1, 1)
TODAY = datetime.date.today()

# ---------- AUTH ----------
bq_client = bigquery.Client(project=PROJECT_ID)

# ---------- DETECT INITIAL LOAD ----------
INITIAL_LOAD = False
try:
    bq_client.get_table(f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}")
    logging.info(f"Table exists → running incremental append.")
    print("✅ BigQuery table found, running incremental append.")
except NotFound:
    INITIAL_LOAD = True
    logging.info(f"Table not found → running full load from {DEFAULT_START_DATE.isoformat()}.")
    print("⚠️ BigQuery table NOT found, running full load.")

# ---------- RATE LIMITER ----------
class RateLimiter:
    def __init__(self, max_calls_per_period=50, period=1.0):
        self.max_calls = max_calls_per_period
        self.period = period
        self.calls_timestamps = []
        self.lock = Lock()

    def acquire(self):
        with self.lock:
            now = time.time()
            self.calls_timestamps = [ts for ts in self.calls_timestamps if now - ts < self.period]
            if len(self.calls_timestamps) >= self.max_calls:
                sleep_time = (self.calls_timestamps[0] + self.period) - now
                logging.info(f"⏱ Rate limit reached. Sleeping {sleep_time:.2f}s")
                print(f"⏱ Rate limit reached. Sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
            self.calls_timestamps.append(time.time())

rate_limiter = RateLimiter(max_calls_per_period=50, period=1.0)

# ---------- HELPERS ----------
def get_tickers() -> list[str]:
    tickers = []
    if not INITIAL_LOAD:
        try:
            query = f"""
                SELECT DISTINCT ticker
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`
                WHERE ticker IS NOT NULL AND ticker != ''
                ORDER BY ticker
            """
            logging.info(f"Querying distinct tickers from BigQuery")
            print("🔍 Querying tickers from BigQuery ...")
            rows = bq_client.query(query).result(timeout=180)
            tickers = [r.ticker for r in rows]
            logging.info(f"Found {len(tickers)} tickers in BigQuery")
            print(f"✅ {len(tickers)} tickers loaded from BigQuery.")
        except Exception as e:
            logging.warning(f"Could not fetch tickers from BigQuery: {e}")
            print(f"⚠️ Could not fetch tickers from BigQuery: {e}")

    if not tickers:
        try:
            from google.cloud import storage
            print("🔍 Loading tickerlist from GCS ...")
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.bucket("profit-scout-data")
            blob = bucket.blob(TICKER_LIST_PATH)
            temp_file = "/tmp/tickerlist.xlsx"
            blob.download_to_filename(temp_file)
            df = pd.read_excel(temp_file)
            os.remove(temp_file)
            tickers = df["Ticker"].dropna().astype(str).str.upper().tolist()
            logging.info(f"Loaded {len(tickers)} tickers from GCS")
            print(f"✅ {len(tickers)} tickers loaded from GCS.")
        except Exception as e:
            logging.error(f"Failed to load tickers from GCS: {e}")
            print(f"❌ Failed to load tickers from GCS: {e}")

    if not tickers:
        logging.error("No tickers found – exiting.")
        print("❌ No tickers found – exiting.")
    return tickers

def get_max_date(ticker: str) -> datetime.date | None:
    try:
        query = f"""
            SELECT MAX(date) AS max_date
            FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`
            WHERE ticker = @ticker
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
        )
        res = bq_client.query(query, job_config=job_config).result(timeout=60)
        max_date = next(res).max_date
        if max_date:
            return max_date.date() if hasattr(max_date, "date") else max_date
    except Exception as e:
        logging.error(f"Error fetching max date for {ticker}: {e}")
        print(f"⚠️ Error fetching max date for {ticker}: {e}")
    return None

def process_fmp_data(json_list: list[dict], ticker: str) -> pd.DataFrame:
    if not json_list:
        return pd.DataFrame()
    df = pd.DataFrame(json_list)
    df = df.rename(columns={"adjClose": "adj_close"})
    df["ticker"] = ticker
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    cols = ["ticker", "date", "open", "high", "low", "adj_close", "volume"]
    available = [c for c in cols if c in df.columns]
    df = df[available]
    for c in ["open", "high", "low", "adj_close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna()
    return df

def fetch_prices_for_ticker(ticker: str, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    if start_date >= end_date:
        return pd.DataFrame()
    start_str = start_date.isoformat()
    to_str = end_date.isoformat()
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?from={start_str}&to={to_str}&apikey={FMP_API_KEY}"
    rate_limiter.acquire()
    try:
        logging.info(f"Fetching {ticker} from {start_str} to {to_str}")
        print(f"🌐 Fetching {ticker} from {start_str} to {to_str}")
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        hist = data.get("historical", [])
        if not hist:
            logging.info(f"{ticker}: no data returned by FMP.")
            print(f"{ticker}: no data returned by FMP.")
            return pd.DataFrame()
        df = process_fmp_data(hist, ticker)
        logging.info(f"{ticker}: fetched {len(df)} new rows.")
        print(f"✅ {ticker}: {len(df)} new rows fetched.")
        return df
    except requests.HTTPError as http_err:
        logging.error(f"{ticker}: HTTP error → {http_err}")
        print(f"{ticker}: HTTP error → {http_err}")
    except Exception as e:
        logging.error(f"{ticker}: fetch failed → {e}")
        print(f"{ticker}: fetch failed → {e}")
    return pd.DataFrame()

def update_prices_for_ticker(ticker: str) -> pd.DataFrame:
    logging.info(f"▶ Updating {ticker} …")
    print(f"▶ Updating {ticker} …")
    
    start_date = DEFAULT_START_DATE  # Default to the earliest desired date

    if not INITIAL_LOAD:
        max_date = get_max_date(ticker)
        if max_date:
            # If we have data, start from the next day
            start_date = max_date + datetime.timedelta(days=1)
        # If no max_date is found, we keep the original DEFAULT_START_DATE
        # to ensure a full backfill for new tickers.

    end_date = TODAY
    df_prices = fetch_prices_for_ticker(ticker, start_date, end_date)
    return df_prices

# ---------- MULTITHREADING WRAPPER ----------
def process_ticker_threadsafe(tk):
    try:
        df_new = update_prices_for_ticker(tk)
        if not df_new.empty:
            logging.info(f"{tk}: processed {len(df_new)} new rows.")
            print(f"✅ {tk}: processed {len(df_new)} new rows.")
            return df_new
        else:
            logging.info(f"{tk}: no new data.")
            print(f"{tk}: no new data.")
    except Exception as e:
        logging.error(f"Failure on {tk}: {e}")
        print(f"❌ Failure on {tk}: {e}")
    return None

# ───────────────────────────────────────────────────────────────────────────
# PRODUCER –  HTTP; publishes one Pub/Sub message per ticker
# ───────────────────────────────────────────────────────────────────────────
def produce(request):
    tickers = get_tickers()
    if not tickers:
        return "No tickers", 200

    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), "fmp-price-sync")

    for t in tickers:
        publisher.publish(topic_path, json.dumps({"ticker": t}).encode())

    return f"Queued {len(tickers)} tickers", 200


# ───────────────────────────────────────────────────────────────────────────
# WORKER –  Pub/Sub; handles exactly ONE ticker
# ───────────────────────────────────────────────────────────────────────────
def worker(event, context):
    try:
        payload = json.loads(base64.b64decode(event["data"]).decode())
        ticker  = payload["ticker"].upper()

        df_new = update_prices_for_ticker(ticker)
        if df_new.empty:
            logging.info(f"{ticker}: no new rows")
            return f"0 rows for {ticker}"

        # ---- write to BigQuery (WRITE_APPEND) ----
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=[
                bigquery.SchemaField("ticker", "STRING"),
                bigquery.SchemaField("date", "TIMESTAMP"),
                bigquery.SchemaField("open", "FLOAT"),
                bigquery.SchemaField("high", "FLOAT"),
                bigquery.SchemaField("low", "FLOAT"),
                bigquery.SchemaField("adj_close", "FLOAT"),
                bigquery.SchemaField("volume", "INTEGER"),
            ],
        )
        job = bq_client.load_table_from_dataframe(
            df_new,
            f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
            job_config=job_config,
        )
        job.result(timeout=300)
        logging.info(f"{ticker}: loaded {job.output_rows} rows")
        return f"{job.output_rows} rows for {ticker}"

    except Exception as e:
        logging.exception(f"Worker failure for {payload if 'payload' in locals() else 'unknown'}")
        raise e          # Pub/Sub triggers retry

# ---------- MAIN ----------
def main(request, context=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    print("🚀 Starting price data updater ...")
    tickers = get_tickers()
    if not tickers:
        print("❌ No tickers loaded. Exiting.")
        return

    print(f"⏩ Processing {len(tickers)} tickers with 24 threads ...")
    all_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
        future_to_ticker = {executor.submit(process_ticker_threadsafe, tk): tk for tk in tickers}
        for i, future in enumerate(concurrent.futures.as_completed(future_to_ticker), 1):
            tk = future_to_ticker[future]
            try:
                result = future.result()
                if result is not None and not result.empty:
                    all_data.append(result)
                print(f"Progress: {i}/{len(tickers)} tickers processed")
            except Exception as exc:
                logging.error(f"Ticker {tk} generated an exception: {exc}")
                print(f"❌ Ticker {tk} generated an exception: {exc}")

    if not all_data:
        logging.info("No new data to write; exiting.")
        print("ℹ️ No new data to write; exiting.")
        return

    final_df = pd.concat(all_data, ignore_index=True)
    logging.info(f"Prepared {len(final_df)} rows to load")
    print(f"📦 Prepared {len(final_df)} rows to load into BigQuery.")

    disposition = "WRITE_TRUNCATE" if INITIAL_LOAD else "WRITE_APPEND"
    job_config = bigquery.LoadJobConfig(
        write_disposition=disposition,
        schema=[
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("date", "TIMESTAMP"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("adj_close", "FLOAT"),
            bigquery.SchemaField("volume", "INTEGER"),
        ],
    )
    logging.info(f"Loading data with {disposition} …")
    print(f"⬆️ Loading data into BigQuery with disposition={disposition} ...")
    job = bq_client.load_table_from_dataframe(
        final_df, f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}", job_config=job_config
    )
    job.result(timeout=300)
    logging.info(f"✅ Loaded {job.output_rows} rows.")
    print(f"✅ Loaded {job.output_rows} rows into BigQuery.")
    print("🎉 === Price update complete ===")
