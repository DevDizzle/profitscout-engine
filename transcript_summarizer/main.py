# transcript_summarizer/main.py
"""
HTTP-triggered Cloud Function (gen 2) that batch-processes every transcript
JSON in `earnings-call-transcripts/`, generating any missing summaries.
"""
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Request
from .core import config, gcs, utils, orchestrator

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── worker -----------------------------------------------------------
def _process_blob(blob_name: str, ticker: str, date: str):
    """Download → summarise → upload (skip if already exists)."""
    out_blob = f"{config.GCS_OUTPUT_PREFIX}{ticker}_{date}.txt"
    if gcs.blob_exists(config.GCS_BUCKET, out_blob):
        log.info("✓ %s already summarised", out_blob)
        return "skipped"

    try:
        text = utils.read_transcript(config.GCS_BUCKET, blob_name)
        summary = orchestrator.summarise(text, ticker=ticker, date=date)
        gcs.write_text(config.GCS_BUCKET, out_blob, summary)
        log.info("✔ %s written", out_blob)
        return "processed"
    except Exception as e:
        log.error("✗ %s failed – %s", blob_name, e, exc_info=True)
        return "error"

# ── HTTP entry-point -------------------------------------------------
def create_transcript_summaries(request: Request):
    """
    No request body needed.
    Returns {processed, skipped, errors}.
    """
    log.info("Batch summariser triggered")

    ticker_filter = utils.load_ticker_set(config.GCS_BUCKET)
    if ticker_filter:
        log.info("Ticker filter enabled (%d tickers)", len(ticker_filter))

    blobs = list(gcs.list_blobs(config.GCS_BUCKET, config.GCS_INPUT_PREFIX))
    if not blobs:
        return (json.dumps({"message": "no transcripts found"}), 200,
                {"Content-Type": "application/json"})

    processed = skipped = errors = 0
    with ThreadPoolExecutor(max_workers=config.MAX_THREADS) as pool:
        futures = {}
        for b in blobs:
            if not b.name.endswith(".json"):
                continue
            ticker, date = utils.parse_filename(b.name)
            if not ticker:
                log.warning("skip malformed filename %s", b.name)
                continue
            if ticker_filter and ticker not in ticker_filter:
                continue
            futures[pool.submit(_process_blob, b.name, ticker, date)] = b.name

        for fut in as_completed(futures):
            result = fut.result()
            if result == "processed":
                processed += 1
            elif result == "skipped":
                skipped += 1
            else:
                errors += 1

    payload = {"processed": processed, "skipped": skipped, "errors": errors}
    log.info("Done – %s", payload)
    return (json.dumps(payload), 200, {"Content-Type": "application/json"})