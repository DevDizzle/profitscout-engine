# transcript_summarizer/core/utils.py
"""
Helper functions used by main.py
- load_ticker_set : read optional tickerlist.txt for filtering
- parse_filename  : extract ticker & date from "<TICKER>_YYYY-MM-DD.json"
- read_transcript : return transcript text from JSON blob
"""
import json
import os
import re
from . import config, gcs

# -------- ticker filter ------------------------------------------
def load_ticker_set(bucket: str) -> set[str]:
    """
    If config.TICKER_LIST_PATH exists in GCS, return the set of tickers inside.
    Otherwise return an empty set (meaning no filtering).
    """
    path = config.TICKER_LIST_PATH
    if not path:
        return set()

    try:
        content = gcs.read_blob(bucket, path)
        return {t.strip().upper() for t in content.splitlines() if t.strip()}
    except Exception:
        # file missing or unreadable → treat as "no filter"
        return set()

# -------- filename parsing ---------------------------------------
_RE = re.compile(r"^([A-Z]+)_(\d{4}-\d{2}-\d{2})\.json$")

def parse_filename(blob_name: str):
    """
    Return (ticker, date) if filename matches the expected pattern,
    otherwise (None, None).
    """
    m = _RE.search(os.path.basename(blob_name))
    return (m.group(1), m.group(2)) if m else (None, None)

# -------- transcript extraction ----------------------------------
def read_transcript(bucket: str, blob_name: str) -> str:
    """
    Download the JSON transcript blob and return the raw text inside.
    Supports several common key names.
    """
    raw = gcs.read_blob(bucket, blob_name)
    try:
        data = json.loads(raw)
        for key in ("content", "transcript", "text", "body"):
            if isinstance(data, dict) and key in data and data[key]:
                return data[key]
    except json.JSONDecodeError:
        pass
    # fallback: treat raw file text as the transcript
    return raw
