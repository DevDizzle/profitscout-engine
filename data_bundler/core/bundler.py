#!/usr/bin/env python3
"""
Core logic for the Data Bundler service.
- Fetches artifacts from GCS.
- Curates and combines them into a clean, token-efficient JSON file.
- Uploads the result to a destination GCS bucket.
- Updates a metadata table in BigQuery with the bundle's location.
"""
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from google.cloud import bigquery, storage
from google.cloud import pubsub_v1

from config import (
    BUCKET_NAME,
    BUNDLE_OUTPUT_BUCKET,
    BUNDLE_OUTPUT_FOLDER,
    DESTINATION_BQ_DATASET,
    DESTINATION_BQ_PROJECT,
    DESTINATION_BQ_TABLE,
    SECTION_GCS_PATHS,
    SOURCE_BQ_DATASET,
    SOURCE_BQ_PROJECT,
    SOURCE_BQ_TABLE,
    TICKER_LIST_PATH,
)

DATE_PAT = re.compile(r"_(\d{4}-\d{2}-\d{2})")

def get_ticker_list(bucket: storage.Bucket) -> List[str]:
    """Loads a list of tickers from a text file in GCS."""
    blob = bucket.blob(TICKER_LIST_PATH)
    if not blob.exists():
        print(f"[ERROR] Ticker file not found at: {TICKER_LIST_PATH}")
        return []
    try:
        content = blob.download_as_text(encoding="utf-8")
        tickers = [
            line.strip().upper() for line in content.splitlines() if line.strip()
        ]
        print(f"[INFO] Loaded {len(tickers)} tickers from gs://{BUCKET_NAME}/{TICKER_LIST_PATH}")
        return tickers
    except Exception as e:
        print(f"[ERROR] Failed to load or parse tickers from GCS: {e}")
        return []

# --- Data Curation Helpers ---

def _publish_completion_message(ticker: str):
    """Publishes a message to the bundle-updated topic."""
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(DESTINATION_BQ_PROJECT, "bundle-updated")
        message_data = json.dumps({"ticker": ticker}).encode("utf-8")
        future = publisher.publish(topic_path, message_data)
        future.result() # Wait for publish to complete
        print(f"[INFO] Published completion message for {ticker} to 'bundle-updated' topic.")
    except Exception as e:
        print(f"[ERROR] Failed to publish completion message for {ticker}: {e}")

def _curate_financial_statements(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extracts key fields from the raw financial statements JSON."""
    curated_statements = []
    reports = raw_data.get("quarterly_reports", raw_data if isinstance(raw_data, list) else [])
    for report in reports:
        income_statement = report.get("income_statement", {})
        balance_sheet = report.get("balance_sheet", {})
        cash_flow = report.get("cash_flow_statement", {})
        curated_statements.append({
            "date": report.get("date"), "period": income_statement.get("period"),
            "revenue": income_statement.get("revenue"), "netIncome": income_statement.get("netIncome"),
            "eps": income_statement.get("eps"), "totalAssets": balance_sheet.get("totalAssets"),
            "totalLiabilities": balance_sheet.get("totalLiabilities"), "totalDebt": balance_sheet.get("totalDebt"),
            "operatingCashFlow": cash_flow.get("operatingCashFlow"), "freeCashFlow": cash_flow.get("freeCashFlow"),
        })
    return curated_statements

def _curate_key_metrics(metrics_data: List[Dict], ratios_data: List[Dict]) -> List[Dict[str, Any]]:
    """Merges and extracts key fields from metrics and ratios data."""
    ratios_by_date = {item.get('date'): item for item in ratios_data}
    curated_metrics = []
    for metrics_item in metrics_data:
        date = metrics_item.get("date")
        ratios_item = ratios_by_date.get(date, {})
        curated_metrics.append({
            "date": date, "period": metrics_item.get("period"),
            "marketCap": metrics_item.get("marketCap"), "peRatio": metrics_item.get("peRatio"),
            "priceToSalesRatio": metrics_item.get("priceToSalesRatio"), "pbRatio": metrics_item.get("pbRatio"),
            "debtToEquity": metrics_item.get("debtToEquity"), "roe": metrics_item.get("roe"),
            "grossProfitMargin": ratios_item.get("grossProfitMargin"), "netProfitMargin": ratios_item.get("netProfitMargin"),
            "freeCashFlowPerShare": metrics_item.get("freeCashFlowPerShare"),
        })
    return curated_metrics

def _curate_technicals(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts the most recent technical indicators from the timeseries data.
    """
    if not isinstance(raw_data, dict):
        return {}

    # The technicals data is a list of daily records. We only need the latest one.
    technicals_timeseries = raw_data.get("technicals_timeseries")
    if not technicals_timeseries or not isinstance(technicals_timeseries, list):
        return {}

    # The list is sorted with the most recent data point at the end.
    latest_technicals = technicals_timeseries[-1]

    # Map the verbose names from the file to the desired keys in the bundle
    return {
        "symbol": raw_data.get("ticker"),
        "fiftyTwoWeekHigh": latest_technicals.get("52w_high"),
        "fiftyTwoWeekLow": latest_technicals.get("52w_low"),
        "movingAverage": {
            "50_day": latest_technicals.get("SMA_50"),
            "200_day": latest_technicals.get("SMA_200"),
        },
        "rsi": latest_technicals.get("RSI_14"),
        "macd": latest_technicals.get("MACD_12_26_9"),
    }

# --- GCS & BQ Helpers ---

def _extract_date_from_name(blob_name: str) -> str:
    m = DATE_PAT.search(blob_name)
    return m.group(1) if m else "0000-00-00"

def _load_json_blob(bucket: storage.Bucket, blob_name: str) -> Optional[Any]:
    """
    Loads a JSON blob from GCS.
    Includes a fallback for malformed MD&A files to load them as raw text.
    """
    blob = bucket.blob(blob_name)
    if not blob.exists():
        print(f"[WARN] Blob does not exist, skipping: {blob_name}")
        return None

    try:
        # First, try to download and parse as standard JSON
        return json.loads(blob.download_as_bytes())

    except json.JSONDecodeError as json_exc:
        print(f"[ERROR] Failed to parse JSON from {blob_name}: {json_exc}.")

        # If parsing fails, check if it's an MD&A file and try the fallback.
        if "sec-mda" in blob_name:
            print(f"[INFO] Attempting fallback for MD&A file: {blob_name}")
            try:
                # Re-download as raw text
                raw_text = blob.download_as_text(encoding="utf-8")
                # Wrap the raw text into the expected JSON structure
                return {"MD&A": raw_text}
            except Exception as text_exc:
                print(f"[ERROR] Fallback failed. Could not read {blob_name} as raw text: {text_exc}")
                return None
        else:
            # For other file types, the error is likely more critical.
            print(f"[ERROR] No fallback available for this file type. Skipping {blob_name}.")
            return None

    except Exception as exc:
        # Catch any other unexpected errors (e.g., network or permission issues)
        print(f"[FATAL] An unexpected error occurred loading {blob_name}: {exc}")
        return None

def _get_latest_date_for_ticker(bucket: storage.Bucket, prefix: str, ticker: str) -> Optional[str]:
    """Finds the latest date string by scanning filenames in a GCS prefix."""
    blobs = [b for b in bucket.list_blobs(prefix=prefix) if ticker.upper() in b.name.upper()]
    if not blobs:
        print(f"[WARN] No blobs found in '{prefix}' to determine date for {ticker}.")
        return None
    latest_blob = max(blobs, key=lambda b: _extract_date_from_name(b.name))
    return _extract_date_from_name(latest_blob.name)

def _update_asset_metadata_in_bq(ticker: str, latest_date: str, gcs_path: str):
    """Fetches latest metadata and MERGES it into the destination BQ table."""
    try:
        client = bigquery.Client()
        
        # 1. Fetch the latest metadata from the source table
        source_table = f"`{SOURCE_BQ_PROJECT}.{SOURCE_BQ_DATASET}.{SOURCE_BQ_TABLE}`"
        query = f"""
            SELECT *
            FROM {source_table}
            WHERE ticker = @ticker AND quarter_end_date = @latest_date
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
                bigquery.ScalarQueryParameter("latest_date", "DATE", latest_date),
            ]
        )
        source_data = client.query(query, job_config=job_config).to_dataframe()

        if source_data.empty:
            print(f"[ERROR] Could not find source metadata for {ticker} on {latest_date}.")
            return

        metadata = source_data.iloc[0].to_dict()

        # 2. Use MERGE to insert or update the destination table
        dest_table = f"`{DESTINATION_BQ_PROJECT}.{DESTINATION_BQ_DATASET}.{DESTINATION_BQ_TABLE}`"
        merge_sql = f"""
            MERGE {dest_table} T
            USING (SELECT @ticker AS ticker) S
            ON T.ticker = S.ticker
            WHEN MATCHED THEN
                UPDATE SET
                    company_name = @company_name,
                    industry = @industry,
                    sector = @sector,
                    quarter_end_date = @quarter_end_date,
                    earnings_call_date = @earnings_call_date,
                    earnings_year = @earnings_year,
                    earnings_quarter = @earnings_quarter,
                    bundle_gcs_path = @bundle_gcs_path,
                    last_updated = @last_updated
            WHEN NOT MATCHED THEN
                INSERT (ticker, company_name, industry, sector, quarter_end_date, earnings_call_date, earnings_year, earnings_quarter, bundle_gcs_path, last_updated)
                VALUES (@ticker, @company_name, @industry, @sector, @quarter_end_date, @earnings_call_date, @earnings_year, @earnings_quarter, @bundle_gcs_path, @last_updated)
        """
        
        params = [
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
            bigquery.ScalarQueryParameter("company_name", "STRING", metadata.get("company_name")),
            bigquery.ScalarQueryParameter("industry", "STRING", metadata.get("industry")),
            bigquery.ScalarQueryParameter("sector", "STRING", metadata.get("sector")),
            bigquery.ScalarQueryParameter("quarter_end_date", "DATE", metadata.get("quarter_end_date")),
            bigquery.ScalarQueryParameter("earnings_call_date", "DATE", metadata.get("earnings_call_date")),
            bigquery.ScalarQueryParameter("earnings_year", "INT64", metadata.get("earnings_year")),
            bigquery.ScalarQueryParameter("earnings_quarter", "INT64", metadata.get("earnings_quarter")),
            bigquery.ScalarQueryParameter("bundle_gcs_path", "STRING", gcs_path),
            bigquery.ScalarQueryParameter("last_updated", "TIMESTAMP", datetime.utcnow()),
        ]
        
        merge_job = client.query(merge_sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
        merge_job.result() # Wait for the job to complete
        print(f"[SUCCESS] BigQuery metadata updated for {ticker}.")

    except Exception as e:
        print(f"[FATAL] Failed to update BigQuery metadata for {ticker}: {e}")

# --- Main Orchestrator ---

def create_and_upload_bundle(bucket: storage.Bucket, ticker: str) -> None:
    print(f"\n{'='*20} Processing Ticker: {ticker} {'='*20}")
    bundle: Dict[str, Any] = {"ticker": ticker.upper()}
    
    latest_date_str = _get_latest_date_for_ticker(bucket, SECTION_GCS_PATHS["earnings-call-transcripts"], ticker)
    if not latest_date_str or latest_date_str == "0000-00-00":
        print(f"[ERROR] Could not determine a valid latest date for {ticker}. Aborting.")
        return

    print(f"[INFO] Using latest date: {latest_date_str} for financial data.")

    # Load data based on latest date
    earnings_path = f"earnings-call-transcripts/{ticker}_{latest_date_str}.json"
    mda_path = f"sec-mda/{ticker}_{latest_date_str}_10-Q.json"
    statements_path = f"financial-statements/{ticker}_{latest_date_str}.json"
    metrics_path = f"key-metrics/{ticker}_{latest_date_str}.json"
    ratios_path = f"ratios/{ticker}_{latest_date_str}.json"
    business_path = SECTION_GCS_PATHS["sec-business"].format(t=ticker)
    technicals_path = SECTION_GCS_PATHS["technicals"].format(t=ticker)
    prices_path = SECTION_GCS_PATHS["prices"].format(t=ticker)

    bundle["earnings_call_summary"] = _load_json_blob(bucket, earnings_path)
    bundle["management_discussion_and_analysis"] = _load_json_blob(bucket, mda_path)
    bundle["business_profile"] = _load_json_blob(bucket, business_path) or ""
    
    statements_data = _load_json_blob(bucket, statements_path)
    if statements_data: bundle["financial_statements"] = _curate_financial_statements(statements_data)

    metrics_data = _load_json_blob(bucket, metrics_path)
    ratios_data = _load_json_blob(bucket, ratios_path)
    if metrics_data and ratios_data: bundle["key_metrics"] = _curate_key_metrics(metrics_data, ratios_data)

    technicals_data = _load_json_blob(bucket, technicals_path)
    if technicals_data: bundle["technicals"] = _curate_technicals(technicals_data)
        
    prices_data = _load_json_blob(bucket, prices_path)
    if prices_data and "prices" in prices_data: bundle["prices"] = prices_data["prices"]

    # Upload final bundle
    bundle_content = json.dumps(bundle, indent=2)
    destination_bucket = storage.Client().bucket(BUNDLE_OUTPUT_BUCKET)
    gcs_path = f"{BUNDLE_OUTPUT_FOLDER}/{ticker.lower()}_bundle.json"
    blob = destination_bucket.blob(gcs_path)
    blob.upload_from_string(bundle_content, content_type="application/json")
    full_gcs_path = f"gs://{BUNDLE_OUTPUT_BUCKET}/{gcs_path}"
    print(f"[SUCCESS] Bundle for {ticker} uploaded to {full_gcs_path}")

    # Update BigQuery with the new metadata
    _update_asset_metadata_in_bq(ticker, latest_date_str, full_gcs_path)
    # Publish the completion message for the next service
    _publish_completion_message(ticker)