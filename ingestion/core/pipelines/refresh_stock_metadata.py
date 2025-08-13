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

def _process_ticker(
    ticker: str,
    profile_info: dict,
    quarters_to_fetch: set,
    fmp_client: FMPClient
) -> list[dict]:
    """Fetches and processes data for one ticker for only the quarters that need to be fetched."""
    try:
        statements_data = fmp_client._make_request(f"income-statement/{ticker}", params={"period": "quarter", "limit": 24})
        if not isinstance(statements_data, list):
            return []

        statements_map = {
            (int(stmt['calendarYear']), int(stmt['period'].replace('Q', ''))): stmt.get('date')
            for stmt in statements_data
        }
        if not statements_map:
            return []

        ticker_records = []
        for year, quarter in quarters_to_fetch:
            try:
                transcript_data = fmp_client.fetch_transcript(ticker, year, quarter)
                if transcript_data and (earnings_call_date := transcript_data.get('date')):
                    if quarter_end_date := statements_map.get((year, quarter)):
                        ticker_records.append({
                            "ticker": ticker, "company_name": profile_info["company_name"],
                            "industry": profile_info["industry"], "sector": profile_info["sector"],
                            "quarter_end_date": quarter_end_date, "earnings_call_date": earnings_call_date,
                            "earnings_year": year, "earnings_quarter": quarter,
                        })
                time.sleep(0.03)
            except Exception:
                continue
        logging.info(f"Processed {ticker}, found {len(ticker_records)} new metadata records.")
        return ticker_records
    except Exception as e:
        logging.error(f"Failed to process ticker {ticker}: {e}", exc_info=True)
        return []

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
    all_records = []

    max_workers = config.MAX_WORKERS_TIERING.get("refresh_stock_metadata")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {}
        for ticker in tickers:
            if profiles_df.loc[profiles_df["ticker"] == ticker].empty:
                continue
            
            quarters_to_fetch = {
                (year, q) for year, q in quarters_to_check
                if (ticker, year, q) not in existing_metadata
            }

            if not quarters_to_fetch:
                logging.info(f"Ticker {ticker} is already up to date.")
                continue

            profile_info = profiles_df.loc[profiles_df["ticker"] == ticker].iloc[0].to_dict()
            future_to_ticker[executor.submit(_process_ticker, ticker, profile_info, quarters_to_fetch, fmp_client)] = ticker

        for future in as_completed(future_to_ticker):
            if records := future.result():
                all_records.extend(records)

    if not all_records:
        logging.info("No new metadata was processed for any ticker.")
        return

    final_df = pd.DataFrame(all_records)
    final_df["quarter_end_date"] = pd.to_datetime(final_df["quarter_end_date"], errors='coerce').dt.date
    final_df["earnings_call_date"] = pd.to_datetime(final_df["earnings_call_date"], errors='coerce').dt.date
    final_df.dropna(subset=['ticker', 'quarter_end_date', 'earnings_call_date'], inplace=True)
    final_df.drop_duplicates(subset=['ticker', 'quarter_end_date'], inplace=True, keep='first')

    schema = [
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"), bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("industry", "STRING"), bigquery.SchemaField("sector", "STRING"),
        bigquery.SchemaField("quarter_end_date", "DATE", mode="REQUIRED"), bigquery.SchemaField("earnings_call_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("earnings_year", "INTEGER"), bigquery.SchemaField("earnings_quarter", "INTEGER"),
    ]
    temp_table_id = f"{config.MASTER_TABLE_ID}_temp_{int(time.time())}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
    bq_client.load_table_from_dataframe(final_df, temp_table_id, job_config=job_config).result()
    
    merge_sql = f"""
    MERGE `{config.MASTER_TABLE_ID}` T
    USING `{temp_table_id}` S ON T.ticker = S.ticker AND T.quarter_end_date = S.quarter_end_date
    WHEN MATCHED THEN
      UPDATE SET T.earnings_call_date = S.earnings_call_date, T.earnings_year = S.earnings_year, T.earnings_quarter = S.earnings_quarter
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (ticker, company_name, industry, sector, quarter_end_date, earnings_call_date, earnings_year, earnings_quarter)
      VALUES (S.ticker, S.company_name, S.industry, S.sector, S.quarter_end_date, S.earnings_call_date, S.earnings_year, S.earnings_quarter)
    """
    bq_client.query(merge_sql).result()
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    logging.info(f"Successfully merged data into {config.MASTER_TABLE_ID}")

    try:
        topic_path = publisher_client.topic_path(config.PROJECT_ID, "new-metadata-found")
        message_data = json.dumps({"status": "complete", "service": "refresh_stock_metadata"}).encode("utf-8")
        future = publisher_client.publish(topic_path, message_data)
        logging.info(f"Published completion message with ID: {future.result()}")
    except Exception as e:
        logging.error(f"Failed to publish to Pub/Sub: {e}", exc_info=True)