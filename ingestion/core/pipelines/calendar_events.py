# ingestion/core/pipelines/calendar_events.py
import logging
import datetime
import hashlib
from typing import List, Dict, Optional
from google.cloud import bigquery, storage
from ..clients.fmp_client import FMPClient
from .. import config, gcs

logging.basicConfig(level=logging.INFO)

# --------------------------------------------------------------------------------------
# Minimal, forward-looking event schema (rolling next 90 days)
# --------------------------------------------------------------------------------------
def _table_schema() -> List[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("entity", "STRING", mode="NULLABLE"),              # ticker for corporate; NULL for economic
        bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),          # Earnings | Dividend | Split | IPO | Economic
        bigquery.SchemaField("event_name", "STRING", mode="REQUIRED"),          # concise label (e.g., "Q3 Earnings (AMC)", "CPI YoY")
        bigquery.SchemaField("event_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("event_time", "TIMESTAMP", mode="NULLABLE"),       # often unknown; leave NULL
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),              # FMP endpoint name
        bigquery.SchemaField("last_seen", "TIMESTAMP", mode="REQUIRED"),
    ]

def _ensure_table(client: bigquery.Client):
    """Create the destination table if missing (no-op if exists)."""
    table = bigquery.Table(config.CALENDAR_EVENTS_TABLE_ID, schema=_table_schema())
    client.create_table(table, exists_ok=True)

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _iso_utc_now() -> str:
    return datetime.datetime.utcnow().isoformat()

def _only_date(s: Optional[str]) -> Optional[str]:
    """Return YYYY-MM-DD if s is a date or timestamp string; else None."""
    if not s or not isinstance(s, str):
        return None
    try:
        return s.split(" ")[0]
    except Exception:
        return None

def _ts_or_none(date_str: Optional[str], time_str: Optional[str]) -> Optional[str]:
    """
    If both date and time-of-day are known, return an RFC3339 timestamp (UTC) string.
    Most vendor feeds provide BMO/AMC flags, not exact clock times; in that case, return None.
    """
    # We intentionally skip synthesizing times from BMO/AMC to keep semantics clean.
    return None

def _stable_id(*parts: Optional[str]) -> str:
    joined = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()

def _append_row(rows: List[Dict], *, entity: Optional[str], event_type: str,
                event_name: str, event_date: Optional[str], source: str, last_seen: str):
    if not event_date:
        return  # skip items without a date
    event_id = _stable_id(source, event_type, entity or "", event_name, event_date)
    rows.append({
        "event_id": event_id,
        "entity": entity,
        "event_type": event_type,
        "event_name": event_name,
        "event_date": event_date,
        "event_time": None,   # keeping minimal & neutral
        "source": source,
        "last_seen": last_seen,
    })

# --------------------------------------------------------------------------------------
# Load & merge
# --------------------------------------------------------------------------------------
def _merge_rows(client: bigquery.Client, rows: List[Dict]):
    if not rows:
        logging.info("No rows to load/merge.")
        return

    temp_table_id = f"{config.CALENDAR_EVENTS_TABLE_ID}_temp"
    job_config = bigquery.LoadJobConfig(schema=_table_schema(), write_disposition="WRITE_TRUNCATE")
    load_job = client.load_table_from_json(rows, temp_table_id, job_config=job_config)
    load_job.result()

    merge_sql = f"""
    MERGE `{config.CALENDAR_EVENTS_TABLE_ID}` T
    USING `{temp_table_id}` S
    ON T.event_id = S.event_id
    WHEN MATCHED THEN
      UPDATE SET
        entity = S.entity,
        event_type = S.event_type,
        event_name = S.event_name,
        event_date = S.event_date,
        event_time = S.event_time,
        source = S.source,
        last_seen = S.last_seen
    WHEN NOT MATCHED THEN
      INSERT (event_id, entity, event_type, event_name, event_date, event_time, source, last_seen)
      VALUES (S.event_id, S.entity, S.event_type, S.event_name, S.event_date, S.event_time, S.source, S.last_seen)
    """
    client.query(merge_sql).result()
    client.delete_table(temp_table_id, not_found_ok=True)

# --------------------------------------------------------------------------------------
# Main pipeline: next 90 days, minimal schema, daily run
# --------------------------------------------------------------------------------------
def run_pipeline(fmp_client: FMPClient, bq_client: bigquery.Client, storage_client: storage.Client):
    """
    Build a rolling 90-day forward calendar of corporate & economic events.
    - Corporate (earnings/dividends/splits/IPOs) are filtered to tickers in tickerlist.txt.
    - Economic events are included with entity=NULL (neutral).
    - LLMs are NOT used; output is minimal & deterministic.
    """
    today = datetime.date.today()
    end = today + datetime.timedelta(days=90)
    start = today  # forward-only

    _ensure_table(bq_client)
    now_iso = _iso_utc_now()

    tickers_to_track = set(gcs.get_tickers(storage_client))
    if not tickers_to_track:
        logging.warning("No tickers found in tickerlist.txt. Corporate events will be skipped.")

    rows: List[Dict] = []

    # ---------------------------
    # Earnings (corporate)
    # ---------------------------
    try:
        earnings = fmp_client.fetch_calendar("earning_calendar", start, end)
        for e in earnings:
            sym = e.get("symbol")
            if sym and sym in tickers_to_track:
                # event_name: "Qx Earnings (BMO/AMC/TBD)" if fiscal period available; keep concise
                period = (e.get("fiscalDateEnding") or "").strip()
                when = (e.get("time") or "").upper()
                session = "BMO" if "BMO" in when else ("AMC" if "AMC" in when else "TBD")
                if period:
                    event_name = f"{sym} Earnings ({session})"
                else:
                    event_name = f"{sym} Earnings ({session})"
                event_date = _only_date(e.get("date"))
                _append_row(rows,
                            entity=sym,
                            event_type="Earnings",
                            event_name=event_name,
                            event_date=event_date,
                            source="earning_calendar",
                            last_seen=now_iso)
    except Exception as ex:
        logging.exception(f"Earnings fetch failed: {ex}")

    # ---------------------------
    # Dividends (corporate)
    # ---------------------------
    try:
        dividends = fmp_client.fetch_calendar("stock_dividend_calendar", start, end)
        for d in dividends:
            sym = d.get("symbol")
            if sym and sym in tickers_to_track:
                # Use ex-dividend date as the event date (FMP 'date' is ex-date in prior usage)
                event_name = f"{sym} Ex-Dividend"
                event_date = _only_date(d.get("date"))
                _append_row(rows,
                            entity=sym,
                            event_type="Dividend",
                            event_name=event_name,
                            event_date=event_date,
                            source="stock_dividend_calendar",
                            last_seen=now_iso)
    except Exception as ex:
        logging.exception(f"Dividends fetch failed: {ex}")

    # ---------------------------
    # Splits (corporate)
    # ---------------------------
    try:
        splits = fmp_client.fetch_calendar("stock_split_calendar", start, end)
        for s in splits:
            sym = s.get("symbol")
            if sym and sym in tickers_to_track:
                num = s.get("numerator")
                den = s.get("denominator")
                ratio = f"{num}:{den}" if (num and den) else "Split"
                event_name = f"{sym} Stock Split ({ratio})"
                event_date = _only_date(s.get("date"))
                _append_row(rows,
                            entity=sym,
                            event_type="Split",
                            event_name=event_name,
                            event_date=event_date,
                            source="stock_split_calendar",
                            last_seen=now_iso)
    except Exception as ex:
        logging.exception(f"Splits fetch failed: {ex}")

    # ---------------------------
    # IPOs (corporate)
    # ---------------------------
    try:
        ipos = fmp_client.fetch_calendar("ipo_calendar", start, end)
        for i in ipos:
            sym = i.get("symbol")
            if sym and sym in tickers_to_track:
                name = (i.get("company") or sym).strip()
                event_name = f"{name} IPO"
                event_date = _only_date(i.get("date"))
                _append_row(rows,
                            entity=sym,
                            event_type="IPO",
                            event_name=event_name,
                            event_date=event_date,
                            source="ipo_calendar",
                            last_seen=now_iso)
    except Exception as ex:
        logging.exception(f"IPOs fetch failed: {ex}")

    # ---------------------------
    # Economic (broad market)
    # ---------------------------
    try:
        economic = fmp_client.fetch_calendar("economic_calendar", start, end)
        for e in economic:
            event_label = (e.get("event") or "").strip()
            if not event_label:
                continue
            event_date = _only_date(e.get("date"))
            # Keep minimal: entity=NULL for economic
            _append_row(rows,
                        entity=None,
                        event_type="Economic",
                        event_name=event_label,
                        event_date=event_date,
                        source="economic_calendar",
                        last_seen=now_iso)
    except Exception as ex:
        logging.exception(f"Economic fetch failed: {ex}")

    if not rows:
        logging.info("No upcoming events found in the next 90 days.")
        return

    _merge_rows(bq_client, rows)
    logging.info(f"Upserted {len(rows)} upcoming events (next 90 days) into {config.CALENDAR_EVENTS_TABLE_ID}.")
