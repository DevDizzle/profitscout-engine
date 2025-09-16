# ingestion/core/pipelines/refresh_stock_metadata.py
import logging
import time
import json
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery, storage, pubsub_v1
from .. import config
from ..gcs import get_tickers
from ..clients.fmp_client import FMPClient

# --- NEW: Define a batch size for processing ---
# We will only process this many missing transcripts per run to avoid timeouts.
PROCESSING_BATCH_SIZE = 2000

def _get_last_n_quarters(n: int = 12) -> set[tuple[int, int]]:
    """Generates a set of the last n (year, quarter) tuples."""
    quarters = set()
    current_date = date.today()
    for _ in range(n):
        current_date -= relativedelta(months=3)
        quarter = (current_date.month - 1) // 3 + 1
        quarters.add((current_date.year, quarter))
    return quarters

def _get_existing_metadata(bq_client: bigquery.Client, tickers: list[str]) -> set:
    """Queries BigQuery for metadata that already exists for the given tickers."""
    if not tickers:
        return set()
    query = f"""
        SELECT ticker, earnings_year, earnings_quarter
        FROM `{config.MASTER_TABLE_ID}`
        WHERE ticker IN UNNEST(@tickers)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tickers", "STRING", tickers)]
    )
    try:
        df = bq_client.query(query, job_config=job_config).to_dataframe()
        return {
            (row['ticker'], row['earnings_year'], row['earnings_quarter'])
            for _, row in df.iterrows()
        }
    except Exception as e:
        logging.error(f"Could not query BigQuery for existing metadata: {e}")
        return set()

def _fetch_profiles_bulk(symbols: list[str], fmp_client: FMPClient) -> pd.DataFrame:
    """Retrieve company profiles for a list of tickers in batches."""
    all_profiles = []
    for i in range(0, len(symbols), 200):
        batch = symbols[i:i + 200]
        try:
            data = fmp_client._make_request(f"profile/{','.join(batch)}", params={})
            if isinstance(data, list) and data:
                df = pd.DataFrame(data)
                df = df[["symbol", "companyName", "industry", "sector"]].rename(
                    columns={"symbol": "ticker", "companyName": "company_name"}
                )
                all_profiles.append(df)
        except Exception as e:
            logging.warning(f"Profile fetch failed for batch starting with {batch[0]}: {e}")
        time.sleep(0.05)
    return pd.concat(all_profiles, ignore_index=True) if all_profiles else pd.DataFrame()

def _fetch_transcripts_bulk(
    work_items: list[tuple[str, int, int]],
    fmp_client: FMPClient
) -> list[dict]:
    """Fetches earnings call transcripts in bulk using a thread pool."""
    records = []
    # Use a slightly higher number of workers to speed up the fetching process
    max_workers = config.MAX_WORKERS_TIERING.get("refresh_stock_metadata", 8)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {
            executor.submit(fmp_client.fetch_transcript, item[0], item[1], item[2]): item
            for item in work_items
        }
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            ticker, year, quarter = item
            try:
                transcript_data = future.result()
                if transcript_data and (earnings_call_date := transcript_data.get("date")):
                    records.append({
                        "ticker": ticker,
                        "earnings_year": year,
                        "earnings_quarter": quarter,
                        "quarter_end_date": transcript_data.get("fillingDate"),
                        "earnings_call_date": earnings_call_date,
                    })
            except Exception as e:
                logging.error(f"Transcript fetch failed for {ticker} {year} Q{quarter}: {e}")
    return records

def run_pipeline(fmp_client: FMPClient, bq_client: bigquery.Client, storage_client: storage.Client, publisher_client: pubsub_v1.PublisherClient):
    """Refreshes the stock metadata table in BigQuery by only fetching missing data."""
    tickers = get_tickers(storage_client)
    if not tickers:
        logging.critical("Metadata: Ticker list is empty. Aborting.")
        return

    profiles_df = _fetch_profiles_bulk(tickers, fmp_client)
    if profiles_df.empty:
        logging.critical("Metadata: Profile fetch failed. Aborting.")
        return

    existing_metadata = _get_existing_metadata(bq_client, tickers)
    quarters_to_check = _get_last_n_quarters(12)

    work_items = []
    for ticker in tickers:
        for year, quarter in quarters_to_check:
            if (ticker, year, quarter) not in existing_metadata:
                work_items.append((ticker, year, quarter))

    if not work_items:
        logging.info("All stock metadata is up-to-date.")
        return

    total_missing = len(work_items)
    logging.info(f"Found {total_missing} total missing transcript dates to fetch.")

    # --- MODIFIED: Process only a batch of the work items ---
    batch_to_process = work_items[:PROCESSING_BATCH_SIZE]
    logging.info(f"Processing a batch of {len(batch_to_process)} items in this run.")

    transcript_records = _fetch_transcripts_bulk(batch_to_process, fmp_client)
    if not transcript_records:
        logging.warning("No new transcript data was fetched in this batch. Exiting.")
        return

    transcripts_df = pd.DataFrame(transcript_records)
    final_df = pd.merge(transcripts_df, profiles_df, on="ticker", how="left")

    final_df["quarter_end_date"] = pd.to_datetime(final_df["quarter_end_date"], errors='coerce').dt.date
    final_df["earnings_call_date"] = pd.to_datetime(final_df["earnings_call_date"], errors='coerce').dt.date
    final_df.dropna(subset=['ticker', 'quarter_end_date', 'earnings_call_date'], inplace=True)
    final_df.drop_duplicates(subset=['ticker', 'quarter_end_date'], inplace=True, keep='first')

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
    
    temp_table_id = f"{config.MASTER_TABLE_ID}_temp_{int(time.time())}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
    bq_client.load_table_from_dataframe(final_df, temp_table_id, job_config=job_config).result()

    merge_sql = f"""
    MERGE `{config.MASTER_TABLE_ID}` T
    USING `{temp_table_id}` S ON T.ticker = S.ticker AND T.quarter_end_date = S.quarter_end_date
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (ticker, company_name, industry, sector, quarter_end_date, earnings_call_date, earnings_year, earnings_quarter)
      VALUES (S.ticker, S.company_name, S.industry, S.sector, S.quarter_end_date, S.earnings_call_date, S.earnings_year, S.earnings_quarter)
    """
    bq_client.query(merge_sql).result()
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    
    remaining_items = total_missing - len(batch_to_process)
    logging.info(f"Successfully merged {len(final_df)} new records. Approximately {remaining_items} items remaining for future runs.")

    # Only publish the completion message when all work is done
    if remaining_items <= 0:
        try:
            topic_path = publisher_client.topic_path(config.PROJECT_ID, "new-metadata-found")
            message_data = json.dumps({"status": "complete", "service": "refresh_stock_metadata"}).encode("utf-8")
            future = publisher_client.publish(topic_path, message_data)
            logging.info(f"Published FINAL completion message with ID: {future.result()}")
        except Exception as e:
            logging.error(f"Failed to publish to Pub/Sub: {e}", exc_info=True)