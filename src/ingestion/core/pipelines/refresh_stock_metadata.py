# ingestion/core/pipelines/refresh_stock_metadata.py
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import pandas as pd
from google.cloud import bigquery, pubsub_v1, storage

from .. import config
from ..clients.fmp_client import FMPClient
from ..gcs import get_tickers

# --- NEW: Define a batch size for processing ---
# We will only process this many missing transcripts per run to avoid timeouts.
PROCESSING_BATCH_SIZE = 2000


def _get_existing_metadata_status(
    bq_client: bigquery.Client, tickers: list[str]
) -> dict:
    """
    Queries BigQuery for the latest earnings_call_date for the given tickers.
    Returns a dict: {ticker: latest_earnings_call_date (date object) or None}
    """
    if not tickers:
        return {}

    # We want to know the *latest* call date we have for each ticker
    query = f"""
        SELECT ticker, MAX(earnings_call_date) as last_call_date
        FROM `{config.MASTER_TABLE_ID}`
        WHERE ticker IN UNNEST(@tickers)
        GROUP BY ticker
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tickers", "STRING", tickers)]
    )

    status = dict.fromkeys(tickers)
    try:
        df = bq_client.query(query, job_config=job_config).to_dataframe()
        for _, row in df.iterrows():
            if row["last_call_date"]:
                status[row["ticker"]] = row["last_call_date"]
    except Exception as e:
        # If table doesn't exist or other error, assume no data
        logging.warning(
            f"Could not query BigQuery for existing metadata (might be empty/missing): {e}"
        )

    return status


def _fetch_profiles_bulk(symbols: list[str], fmp_client: FMPClient) -> pd.DataFrame:
    """Retrieve company profiles for a list of tickers in batches."""
    all_profiles = []
    for i in range(0, len(symbols), 200):
        batch = symbols[i : i + 200]
        try:
            data = fmp_client._make_request(f"profile/{','.join(batch)}", params={})
            if isinstance(data, list) and data:
                df = pd.DataFrame(data)
                # Keep relevant fields
                cols = ["symbol", "companyName", "industry", "sector"]
                # Filter for cols that exist in data
                existing_cols = [c for c in cols if c in df.columns]
                df = df[existing_cols].rename(
                    columns={"symbol": "ticker", "companyName": "company_name"}
                )
                all_profiles.append(df)
        except Exception as e:
            logging.warning(
                f"Profile fetch failed for batch starting with {batch[0]}: {e}"
            )
        time.sleep(0.05)
    return (
        pd.concat(all_profiles, ignore_index=True) if all_profiles else pd.DataFrame()
    )


def _fetch_latest_transcripts_bulk(
    tickers: list[str], fmp_client: FMPClient
) -> list[dict]:
    """Fetches the LATEST earnings call transcript for each ticker using a thread pool."""
    records = []
    max_workers = config.MAX_WORKERS_TIERING.get("refresh_stock_metadata", 8)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {
            executor.submit(fmp_client.get_latest_transcript, ticker): ticker
            for ticker in tickers
        }
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                transcript_data = future.result()
                # transcript_data should be a dict with 'date', 'year', 'quarter', etc.
                if transcript_data and (
                    earnings_call_date := transcript_data.get("date")
                ):
                    records.append(
                        {
                            "ticker": ticker,
                            "earnings_year": transcript_data.get("year"),
                            "earnings_quarter": transcript_data.get("quarter"),
                            "quarter_end_date": transcript_data.get(
                                "fillingDate"
                            ),  # FMP often uses fillingDate or date roughly
                            # Note: FMP transcript object usually has 'date' (call date).
                            # 'fillingDate' might be missing, checking...
                            # If fillingDate missing, we can sometimes infer or leave null,
                            # but schema requires quarter_end_date.
                            # In FMP transcript endpoint, sometimes we don't get exact quarter end date.
                            # We will use 'date' (call date) as fallback or leave it to be cleaned.
                            "earnings_call_date": earnings_call_date,
                        }
                    )
            except Exception as e:
                logging.error(f"Latest transcript fetch failed for {ticker}: {e}")
    return records


def run_pipeline(
    fmp_client: FMPClient,
    bq_client: bigquery.Client,
    storage_client: storage.Client,
    publisher_client: pubsub_v1.PublisherClient,
):
    """
    Refreshes the stock metadata table by fetching the LATEST transcript for tickers
    that are missing data or have stale data (older than 75 days).
    Enforces 1 ticker per row (latest wins).
    """
    tickers = get_tickers(storage_client)
    if not tickers:
        logging.critical("Metadata: Ticker list is empty. Aborting.")
        return

    # 1. Check what we have (Last Call Date)
    existing_status = _get_existing_metadata_status(bq_client, tickers)

    # 2. Identify stale tickers
    # Stale if:
    #   - Not in DB (None)
    #   - Last call date is > 75 days ago (implying a new quarter might be available)
    today = date.today()
    stale_threshold = today - timedelta(days=75)

    work_items = []
    for ticker in tickers:
        last_date = existing_status.get(ticker)
        if last_date is None:
            # Priority 0: Missing completely
            work_items.append((ticker, date.min))
        elif last_date < stale_threshold:
            # Priority 1: Stale (older date first)
            work_items.append((ticker, last_date))

    if not work_items:
        logging.info("All stock metadata is up-to-date (no tickers > 75 days old).")
        return

    # 3. Sort work items by date ascending (oldest/missing first)
    work_items.sort(key=lambda x: x[1])

    # 4. Batching
    batch_tuples = work_items[:PROCESSING_BATCH_SIZE]
    batch_tickers = [t[0] for t in batch_tuples]

    logging.info(
        f"Found {len(work_items)} stale tickers. Processing batch of {len(batch_tickers)}."
    )

    # 5. Fetch Data
    profiles_df = _fetch_profiles_bulk(batch_tickers, fmp_client)
    transcript_records = _fetch_latest_transcripts_bulk(batch_tickers, fmp_client)

    if not transcript_records:
        logging.warning("No new transcript data was fetched in this batch. Exiting.")
        return

    transcripts_df = pd.DataFrame(transcript_records)

    # Merge profiles
    if not profiles_df.empty:
        final_df = pd.merge(transcripts_df, profiles_df, on="ticker", how="left")
    else:
        final_df = transcripts_df
        # Add missing columns if profile fetch failed completely (unlikely)
        for col in ["company_name", "industry", "sector"]:
            final_df[col] = None

    # Clean dates
    # Note: FMP 'fillingDate' in transcript endpoint might be datetime string
    final_df["quarter_end_date"] = pd.to_datetime(
        final_df["quarter_end_date"], errors="coerce"
    ).dt.date
    final_df["earnings_call_date"] = pd.to_datetime(
        final_df["earnings_call_date"], errors="coerce"
    ).dt.date

    # Fallback: If quarter_end_date is null (sometimes FMP doesn't send it in transcript),
    # assume it is ~20 days before call date or just use call date as proxy (not ideal but better than drop)
    # But schema requires it. Let's try to fill it.
    # Actually, if we dropna, we might lose data.
    # Let's drop only if earnings_call_date is missing.
    final_df.dropna(subset=["ticker", "earnings_call_date"], inplace=True)

    # If quarter_end_date is missing, infer it: Call Date - 30 days?
    # A safe bet for sorting is just using earnings_call_date if quarter_end is missing.
    final_df["quarter_end_date"] = final_df["quarter_end_date"].fillna(
        final_df["earnings_call_date"]
    )

    # Deduplicate within the batch (keep latest call date)
    final_df.sort_values(by="earnings_call_date", ascending=False, inplace=True)
    final_df.drop_duplicates(subset=["ticker"], keep="first", inplace=True)

    # 6. Load to BigQuery
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
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", schema=schema
    )
    bq_client.load_table_from_dataframe(
        final_df, temp_table_id, job_config=job_config
    ).result()

    # 7. Merge (Upsert)
    # matching on ticker. Updating all fields.
    merge_sql = f"""
    MERGE `{config.MASTER_TABLE_ID}` T
    USING `{temp_table_id}` S ON T.ticker = S.ticker
    WHEN MATCHED THEN
      UPDATE SET
        company_name = S.company_name,
        industry = S.industry,
        sector = S.sector,
        quarter_end_date = S.quarter_end_date,
        earnings_call_date = S.earnings_call_date,
        earnings_year = S.earnings_year,
        earnings_quarter = S.earnings_quarter
    WHEN NOT MATCHED THEN
      INSERT (ticker, company_name, industry, sector, quarter_end_date, earnings_call_date, earnings_year, earnings_quarter)
      VALUES (S.ticker, S.company_name, S.industry, S.sector, S.quarter_end_date, S.earnings_call_date, S.earnings_year, S.earnings_quarter)
    """
    bq_client.query(merge_sql).result()

    # 8. Clean up History (Enforce 1 Ticker Per Row)
    # The MERGE above handles 1-to-1 update if T has unique tickers.
    # To strictly enforce 1 ticker per row (removing duplicates), we rewrite the table
    # keeping only the row with the latest earnings_call_date.
    cleanup_sql = f"""
    CREATE OR REPLACE TABLE `{config.MASTER_TABLE_ID}` AS
    SELECT * EXCEPT(rn)
    FROM (
      SELECT *, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY earnings_call_date DESC) as rn
      FROM `{config.MASTER_TABLE_ID}`
    )
    WHERE rn = 1
    """
    bq_client.query(cleanup_sql).result()

    bq_client.delete_table(temp_table_id, not_found_ok=True)

    remaining_items = len(work_items) - len(batch_tickers)
    logging.info(
        f"Successfully merged {len(final_df)} records. Approx {remaining_items} stale tickers remaining."
    )

    # Publish completion if we are nearly done
    if remaining_items <= 0:
        try:
            topic_path = publisher_client.topic_path(
                config.PROJECT_ID, "new-metadata-found"
            )
            message_data = json.dumps(
                {"status": "complete", "service": "refresh_stock_metadata"}
            ).encode("utf-8")
            future = publisher_client.publish(topic_path, message_data)
            logging.info(
                f"Published FINAL completion message with ID: {future.result()}"
            )
        except Exception as e:
            logging.error(f"Failed to publish to Pub/Sub: {e}", exc_info=True)
