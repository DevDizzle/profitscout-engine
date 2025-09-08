# ingestion/core/pipelines/calendar_events.py
import logging
import datetime
from typing import List, Dict
from google.cloud import bigquery, storage
from ..clients.fmp_client import FMPClient
from .. import config, gcs


def _table_schema() -> List[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("event_type", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("event_date", "DATE"),
        bigquery.SchemaField("ticker", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("impact_level", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("eps_estimated", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("eps_actual", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("revenue_estimated", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("revenue_actual", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("dividend_amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("ex_dividend_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("payment_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("declaration_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("actual_value", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("forecast_value", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("previous_value", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("shares", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("offer_amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("numerator", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("denominator", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("last_updated", "TIMESTAMP"),
        bigquery.SchemaField("additional_details", "JSON", mode="NULLABLE"),
    ]


def _ensure_table(client: bigquery.Client):
    table = bigquery.Table(config.CALENDAR_EVENTS_TABLE_ID, schema=_table_schema())
    client.create_table(table, exists_ok=True)


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_date(date_str: str) -> str | None:
    """Safely formats a string into YYYY-MM-DD, handling timestamps."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        # Split on space to remove the time part if it exists
        return date_str.split(" ")[0]
    except Exception:
        return None


def _load_rows(client: bigquery.Client, rows: List[Dict]):
    temp_table_id = f"{config.CALENDAR_EVENTS_TABLE_ID}_temp"
    job_config = bigquery.LoadJobConfig(schema=_table_schema(), write_disposition="WRITE_TRUNCATE")
    load_job = client.load_table_from_json(rows, temp_table_id, job_config=job_config)
    load_job.result()

    merge_sql = f"""
    MERGE `{config.CALENDAR_EVENTS_TABLE_ID}` T
    USING `{temp_table_id}` S
    ON T.event_type = S.event_type AND T.description = S.description AND T.event_date = S.event_date
    WHEN MATCHED THEN
      UPDATE SET
        ticker = S.ticker,
        country = S.country,
        impact_level = S.impact_level,
        eps_estimated = S.eps_estimated,
        eps_actual = S.eps_actual,
        revenue_estimated = S.revenue_estimated,
        revenue_actual = S.revenue_actual,
        dividend_amount = S.dividend_amount,
        ex_dividend_date = S.ex_dividend_date,
        payment_date = S.payment_date,
        declaration_date = S.declaration_date,
        actual_value = S.actual_value,
        forecast_value = S.forecast_value,
        previous_value = S.previous_value,
        shares = S.shares,
        offer_amount = S.offer_amount,
        numerator = S.numerator,
        denominator = S.denominator,
        source = S.source,
        last_updated = S.last_updated,
        additional_details = S.additional_details
    WHEN NOT MATCHED THEN
      INSERT (event_type, description, event_date, ticker, country, impact_level, eps_estimated, eps_actual, revenue_estimated, revenue_actual, dividend_amount, ex_dividend_date, payment_date, declaration_date, actual_value, forecast_value, previous_value, shares, offer_amount, numerator, denominator, source, last_updated, additional_details)
      VALUES (event_type, description, event_date, ticker, country, impact_level, eps_estimated, eps_actual, revenue_estimated, revenue_actual, dividend_amount, ex_dividend_date, payment_date, declaration_date, actual_value, forecast_value, previous_value, shares, offer_amount, numerator, denominator, source, last_updated, additional_details)
    """
    client.query(merge_sql).result()
    client.delete_table(temp_table_id, not_found_ok=True)


def run_pipeline(fmp_client: FMPClient, bq_client: bigquery.Client, storage_client: storage.Client):
    """Fetches calendar events, filtering for tickers in tickerlist.txt."""
    today = datetime.date.today()
    start = today - datetime.timedelta(days=30)
    end = today + datetime.timedelta(days=90)

    _ensure_table(bq_client)
    now = datetime.datetime.utcnow().isoformat()

    # Get the official list of tickers to filter against
    tickers_to_track = set(gcs.get_tickers(storage_client))
    if not tickers_to_track:
        logging.warning("No tickers found in tickerlist.txt. Only fetching economic data.")

    rows: List[Dict] = []

    # --- Ticker-Specific Events (Filtered) ---
    
    earnings = fmp_client.fetch_calendar("earning_calendar", start, end)
    for e in earnings:
        if e.get("symbol") in tickers_to_track:
            rows.append({
                "event_type": "Earnings",
                "description": f"{e.get('symbol')} Earnings Report",
                "event_date": _format_date(e.get("date")),
                "ticker": e.get("symbol"),
                "eps_estimated": _to_float(e.get("epsEstimated")),
                "eps_actual": _to_float(e.get("eps")),
                "revenue_estimated": _to_float(e.get("revenueEstimated")),
                "revenue_actual": _to_float(e.get("revenue")),
                "source": "earning_calendar",
                "last_updated": now,
                "additional_details": {k: e.get(k) for k in ["time", "updatedFromDate", "fiscalDateEnding"] if e.get(k) is not None},
            })

    dividends = fmp_client.fetch_calendar("stock_dividend_calendar", start, end)
    for d in dividends:
        if d.get("symbol") in tickers_to_track:
            rows.append({
                "event_type": "Dividend",
                "description": f"{d.get('symbol')} Dividend Announcement",
                "event_date": _format_date(d.get("date")),
                "ticker": d.get("symbol"),
                "dividend_amount": _to_float(d.get("adjDividend")),
                "ex_dividend_date": _format_date(d.get("date")),
                "payment_date": _format_date(d.get("paymentDate")),
                "declaration_date": _format_date(d.get("declarationDate")),
                "source": "stock_dividend_calendar",
                "last_updated": now,
                "additional_details": {k: d.get(k) for k in ["label", "recordDate", "dividend"] if d.get(k) is not None},
            })

    ipos = fmp_client.fetch_calendar("ipo_calendar", start, end)
    for i in ipos:
        if i.get("symbol") in tickers_to_track:
            rows.append({
                "event_type": "IPO",
                "description": f"{i.get('company') or i.get('symbol')} IPO",
                "event_date": _format_date(i.get("date")),
                "ticker": i.get("symbol"),
                "shares": _to_int(i.get("shares")),
                "offer_amount": _to_float(i.get("offerAmount")),
                "source": "ipo_calendar",
                "last_updated": now,
                "additional_details": {k: i.get(k) for k in ["exchange", "actions", "priceRangeLow", "priceRangeHigh", "totalSharesValue"] if i.get(k) is not None},
            })

    splits = fmp_client.fetch_calendar("stock_split_calendar", start, end)
    for s in splits:
        if s.get("symbol") in tickers_to_track:
            rows.append({
                "event_type": "Stock Split",
                "description": f"{s.get('symbol')} Stock Split",
                "event_date": _format_date(s.get("date")),
                "ticker": s.get("symbol"),
                "numerator": _to_float(s.get("numerator")),
                "denominator": _to_float(s.get("denominator")),
                "source": "stock_split_calendar",
                "last_updated": now,
                "additional_details": {k: s.get(k) for k in ["label"] if s.get(k) is not None},
            })

    # --- Global Economic Events (Not Filtered) ---
    
    economic = fmp_client.fetch_calendar("economic_calendar", start, end)
    for e in economic:
        rows.append({
            "event_type": "Economic",
            "description": e.get("event"),
            "event_date": _format_date(e.get("date")),
            "country": e.get("country"),
            "impact_level": e.get("impact"),
            "actual_value": _to_float(e.get("actual")),
            "forecast_value": _to_float(e.get("estimate")),
            "previous_value": _to_float(e.get("previous")),
            "source": "economic_calendar",
            "last_updated": now,
            "additional_details": {k: e.get(k) for k in ["change", "changePercentage"] if e.get(k) is not None},
        })

    if not rows:
        logging.info("No calendar events fetched.")
        return

    _load_rows(bq_client, rows)
    logging.info(f"Loaded {len(rows)} calendar events to BigQuery.")