"""Central configuration for all Enrichment services."""

from __future__ import annotations

import json
import os
from datetime import datetime

# --- Global Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- BigQuery ---
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")
# This is the line we're fixing. Renamed from BQ_METADATA_TABLE
STOCK_METADATA_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.stock_metadata"
SCORES_TABLE_NAME = "analysis_scores"
SCORES_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{SCORES_TABLE_NAME}"
OPTIONS_CHAIN_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.options_chain"
OPTIONS_CANDIDATES_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.options_candidates"
PRICE_TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.price_data"

# --- Score Aggregator ---
SCORE_WEIGHTS = {
    "news_score": 0.25,
    "technicals_score": 0.45,
    "mda_score": 0.05,
    "transcript_score": 0.05,
    "financials_score": 0.10,
    "fundamentals_score": 0.10,
}

# --- Vertex AI Gen AI ---
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-2.0-flash")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.6"))
TOP_P = float(os.getenv("TOP_P", "0.95"))
TOP_K = int(os.getenv("TOP_K", "30"))
SEED = int(os.getenv("SEED", "42"))
CANDIDATE_COUNT = int(os.getenv("CANDIDATE_COUNT", "1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "512"))

# --- Cloud Storage Prefixes ---
PREFIXES = {
    "mda_analyzer": {"input": "sec-mda/", "output": "mda-analysis/"},
    "transcript_analyzer": {"input": "earnings-call-transcripts/", "output": "transcript-analysis/"},
    "financials_analyzer": {"input": "financial-statements/", "output": "financials-analysis/"},
    "technicals_analyzer": {"input": "technicals/", "output": "technicals-analysis/"},
    "news_analyzer": {"input": "headline-news/", "output": "news-analysis/"},
    "news_fetcher": {"query_cache": "news-queries/"},
    "business_summarizer": {"input": "sec-business/", "output": "business-summaries/"},
    "fundamentals_analyzer": {
        "input_metrics": "key-metrics/",
        "input_ratios": "ratios/",
        "output": "fundamentals-analysis/"
    },
    "macro_thesis": {"output": "macro-thesis/"},
}

ANALYSIS_PREFIXES = {
    "business_summary": "business-summaries/",
    "news": "news-analysis/",
    "technicals": "technicals-analysis/",
    "mda": "mda-analysis/",
    "transcript": "transcript-analysis/",
    "financials": "financials-analysis/",
    "fundamentals": "fundamentals-analysis/",
    "macro_thesis": "macro-thesis/",
}

# --- Job Parameters ---
MAX_WORKERS = 8
HEADLINE_LIMIT = 25

# Timeout for worker processes in seconds
WORKER_TIMEOUT = 300


# --- Macro Thesis Configuration ---

def _parse_macro_sources(raw: str | None) -> list[dict[str, str]]:
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        urls = [item.strip() for item in raw.split(",") if item.strip()]
        return [{"name": url, "url": url} for url in urls]

    if isinstance(parsed, dict):
        return [{"name": parsed.get("name") or parsed.get("url", "macro-source"), "url": parsed.get("url", ""), **{k: v for k, v in parsed.items() if k not in {"name", "url"}}}]

    if isinstance(parsed, list):
        sources: list[dict[str, str]] = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            name = item.get("name") or item.get("url") or "macro-source"
            url = item.get("url", "")
            cleaned = {k: v for k, v in item.items() if isinstance(k, str)}
            cleaned.update({"name": name, "url": url})
            sources.append(cleaned)
        return sources

    return []


MACRO_THESIS_SOURCES = _parse_macro_sources(os.getenv("MACRO_THESIS_SOURCES"))
MACRO_THESIS_HTTP_TIMEOUT = int(os.getenv("MACRO_THESIS_HTTP_TIMEOUT", "20"))
MACRO_THESIS_MAX_SOURCES = int(os.getenv("MACRO_THESIS_MAX_SOURCES", "5"))
MACRO_THESIS_SOURCE_CHAR_LIMIT = int(os.getenv("MACRO_THESIS_SOURCE_CHAR_LIMIT", "2000"))


def get_macro_thesis_output_prefix() -> str:
    return PREFIXES["macro_thesis"]["output"]


def macro_thesis_blob_name(as_of: datetime | None = None) -> str:
    ts = (as_of or datetime.utcnow()).strftime("%Y-%m-%d")
    return f"{get_macro_thesis_output_prefix()}macro_thesis_{ts}.json"


def get_latest_macro_thesis_blob(bucket_name: str | None = None) -> str | None:
    """Return the latest macro thesis blob path if one exists."""

    try:
        from . import gcs as _gcs
    except Exception:  # pragma: no cover - fallback when gcs unavailable
        return None

    bucket = bucket_name or GCS_BUCKET_NAME
    prefix = get_macro_thesis_output_prefix()
    blobs = _gcs.list_blobs(bucket, prefix=prefix)
    candidates = [b for b in blobs if b.endswith(".json")]
    if not candidates:
        return None
    return max(candidates)
