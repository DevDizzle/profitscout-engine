# ingestion/core/pipelines/calendar_events.py
"""
Builds and maintains a rolling 90-day calendar of market events.

This pipeline fetches upcoming corporate events (earnings, dividends, splits, IPOs)
and broad economic events from the FMP API. It filters corporate events to only
include tickers from the master `tickerlist.txt` and filters economic events
to a curated list of significant US indicators. The collected events are then
upserted into a BigQuery table, creating a forward-looking calendar.
"""
import logging
import datetime
import hashlib
from typing import List, Dict, Optional
from google.cloud import bigquery, storage
from ..clients.fmp_client import FMPClient
from .. import config, gcs

def _table_schema() -> List[bigquery.SchemaField]:
    """Defines the BigQuery schema for the calendar events table."""
    return [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("entity", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("event_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_seen", "TIMESTAMP", mode="REQUIRED"),
    ]

def _ensure_table(client: bigquery.Client):
    """Creates the calendar events BigQuery table if it does not already exist."""
    table = bigquery.Table(config.CALENDAR_EVENTS_TABLE_ID, schema=_table_schema())
    client.create_table(table, exists_ok=True)

def _stable_id(*parts: Optional[str]) -> str:
    """Creates a stable, deterministic hash ID for an event."""
    joined = "|".join(str(p or "") for p in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()

def _truncate_and_load_rows(client: bigquery.Client, rows: List[Dict]):
    """
    Truncates the target table and loads a fresh set of event rows.
    """
    if not rows:
        logging.warning("No calendar event rows to load. The table will be truncated and left empty.")
    
    job_config = bigquery.LoadJobConfig(
        schema=_table_schema(),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    try:
        load_job = client.load_table_from_json(
            rows,
            config.CALENDAR_EVENTS_TABLE_ID,
            job_config=job_config
        )
        load_job.result()  # Wait for the job to complete.
        logging.info(f"Successfully truncated and loaded {load_job.output_rows} rows into {config.CALENDAR_EVENTS_TABLE_ID}.")
    except Exception as e:
        logging.error(f"Failed to truncate and load calendar events: {e}", exc_info=True)
        raise

def run_pipeline(fmp_client: FMPClient, bq_client: bigquery.Client, storage_client: storage.Client):
    """
    Runs the full calendar events collection pipeline.

    Args:
        fmp_client: An initialized FMPClient instance.
        bq_client: An initialized BigQuery client.
        storage_client: An initialized Google Cloud Storage client.
    """
    today = datetime.date.today()
    end_date = today + datetime.timedelta(days=90)
    _ensure_table(bq_client)
    now_iso = datetime.datetime.utcnow().isoformat()

    tickers_to_track = set(gcs.get_tickers(storage_client, config.GCS_BUCKET_NAME, config.TICKER_LIST_PATH))
    if not tickers_to_track:
        logging.warning("No tickers found in tickerlist.txt. Corporate events will be skipped.")

    events_dict: Dict[str, Dict] = {}
    
    event_sources = [
        ("earning_calendar", "Earnings"),
        ("stock_dividend_calendar", "Dividend"),
        ("stock_split_calendar", "Split"),
        ("ipo_calendar", "IPO"),
        ("economic_calendar", "Economic"),
    ]

    # --- THIS IS THE FIX ---
    # The list of significant events has been updated with the new, more specific keywords.
    # The matching logic is now a more robust substring search.
    significant_events = {
        "fed interest rate decision", "fed press conference", "fomc minutes",
        "fomc economic projections", "treasury refunding announcement", "cpi",
        "core cpi yoy", "core pce price index mom", "producer price index", "ppi",
        "non farm payrolls", "unemployment rate", "average hourly wages yoy",
        "retail sales mom", "ism manufacturing pmi", "ism services pmi",
        "jolts job openings", "michigan 5-year inflation expectations",
        "s&p/case-shiller home price index", "gdp growth annualized", "10-year note auction"
    }

    for source_endpoint, event_type in event_sources:
        try:
            events = fmp_client.fetch_calendar(source_endpoint, today, end_date)
            for event in events:
                ticker = event.get("symbol")
                
                if event_type != "Economic" and (not ticker or ticker not in tickers_to_track):
                    continue

                if event_type == "Economic":
                    if event.get("country", "").upper() != "US":
                        continue
                    
                    event_label = event.get("event", "").strip().lower()
                    # The matching logic now checks if any of the significant keywords are
                    # contained within the event name from the API.
                    if not any(keyword in event_label for keyword in significant_events):
                        continue
                
                event_date_str = (event.get("date") or "").split(" ")[0]
                if not event_date_str:
                    continue

                event_name = event.get("event") or f"{ticker} {event_type}"
                event_id = _stable_id(source_endpoint, event_type, ticker, event_name, event_date_str)
                
                events_dict[event_id] = {
                    "event_id": event_id,
                    "entity": ticker if event_type != "Economic" else None,
                    "event_type": event_type,
                    "event_name": event_name,
                    "event_date": event_date_str,
                    "event_time": None,
                    "source": source_endpoint,
                    "last_seen": now_iso,
                }
        except Exception as ex:
            logging.exception(f"Failed to fetch or process {event_type} events: {ex}")

    _truncate_and_load_rows(bq_client, list(events_dict.values()))