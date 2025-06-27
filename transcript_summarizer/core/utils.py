# transcript_summarizer/core/utils.py
import json
import os
import re
from . import gcs, config

_TICKER_RE = re.compile(r"^([A-Z]+)_(\d{4}-\d{2}-\d{2})\.json$")

def load_ticker_set(bucket_name: str) -> set[str]:
    """
    If TICKER_LIST_PATH exists return its tickers; otherwise return empty set
    meaning "no filter".
    """
    if not config.TICKER_LIST_PATH:
        return set()

    try:
        content = gcs.read_blob(bucket_name, config.TICKER_LIST_PATH)
        return {t.strip().upper() for t in content.splitlines() if t.strip()}
    except Exception:
        # no file or unreadable → treat as no filter
        return set()

def parse_filename(blob_name: str):
    """Return (ticker, date) or (None, None) if filename is malformed."""
    m = _TICKER_RE.search(os.path.basename(blob_name))
    return (m.group(1), m.group(2)) if m else (None, None)

def read_transcript(bucket: str, blob_name: str) -> str:
    """Extract the transcript text from JSON (supports several field names)."""
    raw = gcs.read_blob(bucket, blob_name)
    data = json.loads(raw)
    for key in ("content", "transcript", "text", "body"):
        if isinstance(data, dict) and key in data and data[key]:
            return data[key]
    # Fallback – treat entire JSON as text
    return raw
