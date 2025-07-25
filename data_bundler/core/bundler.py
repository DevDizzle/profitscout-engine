#!/usr/bin/env python3
"""
Core logic for the Data Bundler service.
- Fetches artifacts from GCS.
- Combines them into a single JSON file.
- Uploads the result back to GCS.
"""
import json
import re
from pathlib import Path
from typing import Any, Dict, List

from google.cloud import storage

from config import (
    BUNDLE_OUTPUT_FOLDER,
    DOC_DESCRIPTIONS,
    SECTION_GCS_PATHS,
    TICKER_LIST_PATH,
)

DATE_PAT = re.compile(r"_(\d{4}-\d{2}-\d{2})")

def _extract_date(candidate: str) -> str:
    """Extracts a date from a filename using a regex pattern."""
    m = DATE_PAT.search(candidate)
    return m.group(1) if m else "0000-00-00"

def _load_json_blob(bucket: storage.Bucket, blob_name: str) -> Any | None:
    """Loads and parses a JSON blob from GCS."""
    blob = bucket.blob(blob_name)
    try:
        raw_bytes = blob.download_as_bytes()
        raw_text = raw_bytes.decode("utf-8", errors="replace").strip()
        if not raw_text:
            print(f"[WARN] Blob is empty, skipping: {blob_name}")
            return None
        return json.loads(raw_text)
    except Exception as exc:
        print(f"[ERROR] Failed to load JSON from {blob_name}: {exc}")
        return None

def _collect_section_gcs(bucket: storage.Bucket, section: str, ticker: str) -> Dict[str, Any] | None:
    """Collects data for a single section for a given ticker."""
    template = SECTION_GCS_PATHS.get(section)
    if not template:
        print(f"[WARN] Unknown section defined: {section}")
        return None

    path_pattern = template.format(t=ticker)
    documents = None

    # Sections with a single, predictable file
    if section in {"prices", "technicals", "ml_predictions"}:
        if bucket.blob(path_pattern).exists():
            documents = _load_json_blob(bucket, path_pattern)
        else:
            print(f"[DEBUG] Missing single-file artifact for {section}: {path_pattern}")

    # Sections with multiple files in a folder
    elif section in {"financial-statements", "fundamentals"}:
        blobs = [b for b in bucket.list_blobs(prefix=path_pattern) if b.name.endswith(".json")]
        if blobs:
            docs = [_load_json_blob(bucket, b.name) for b in blobs]
            documents = [d for d in docs if d is not None]
            print(f"[DEBUG] Found {len(documents)} documents for {section}")
        else:
            print(f"[DEBUG] No documents found for {section} at {path_pattern}")

    # Text-heavy sections where we only want the latest file
    elif section in {"earnings-call", "sec-mda", "sec-risk", "sec-business"}:
        blobs = [b for b in bucket.list_blobs(prefix=path_pattern) if ticker.lower() in b.name.lower() and b.name.endswith(".json")]
        if blobs:
            latest_blob = max(blobs, key=lambda b: _extract_date(b.name))
            documents = _load_json_blob(bucket, latest_blob.name)
            print(f"[DEBUG] Using latest artifact for {section}: {Path(latest_blob.name).name}")
        else:
            print(f"[DEBUG] No text artifacts found for {section} at {path_pattern}")

    if documents:
        return {"description": DOC_DESCRIPTIONS[section], "documents": documents}
    
    return None

def create_and_upload_bundle(bucket: storage.Bucket, ticker: str) -> None:
    """Orchestrates the creation and upload of a single ticker bundle."""
    print(f"\n{'='*20} Processing Ticker: {ticker} {'='*20}")
    bundle = {"ticker": ticker.upper(), "sections": {}}

    for section in SECTION_GCS_PATHS:
        section_data = _collect_section_gcs(bucket, section, ticker)
        if section_data:
            bundle["sections"][section] = section_data
            print(f"[INFO] Successfully added section: {section}")
        else:
            print(f"[WARN] Could not find data for section, skipping: {section}")

    if not bundle["sections"]:
        print(f"[ERROR] No data found for ticker {ticker}. Aborting bundle creation.")
        return

    # Upload the final bundle to GCS
    bundle_content = json.dumps(bundle, indent=2)
    gcs_path = f"{BUNDLE_OUTPUT_FOLDER}/{ticker.lower()}_bundle.json"
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(bundle_content, content_type="application/json")
    print(f"[SUCCESS] Bundle for {ticker} uploaded to GCS at {gcs_path}")

def get_ticker_list(bucket: storage.Bucket) -> List[str]:
    """Retrieves the list of tickers from tickerlist.txt in GCS."""
    blob = bucket.blob(TICKER_LIST_PATH)
    try:
        content = blob.download_as_text()
        tickers = [line.strip().upper() for line in content.splitlines() if line.strip()]
        print(f"[INFO] Found {len(tickers)} tickers in {TICKER_LIST_PATH}")
        return tickers
    except Exception as e:
        print(f"[ERROR] Could not read ticker list from GCS: {e}")
        return []