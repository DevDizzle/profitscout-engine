"""
Helper functions for parsing MD&A filenames and data.
"""
import json
import os
import re
from datetime import datetime

def parse_filename(blob_name: str) -> tuple[str | None, str | None, str | None, int | None, int | None]:
    """
    Parses filenames like 'AAL_2025-06-30_10-Q.json'.
    Returns (ticker, date_str, filing_type, year, quarter).
    """
    pattern = re.compile(r"([A-Z]+)_(\d{4}-\d{2}-\d{2})_(10-[KQ]T?|20-F)\.json$")
    match = pattern.search(os.path.basename(blob_name))
    if not match:
        return None, None, None, None, None

    ticker, date_str, filing_type = match.groups()
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        # Estimate quarter from the period end date
        quarter = (date_obj.month - 1) // 3 + 1
        year = date_obj.year
        return ticker, date_str, filing_type, year, quarter
    except ValueError:
        return None, None, None, None, None

def read_mda_data(raw_json: str) -> str | None:
    """
    Parses raw JSON from GCS to extract the MD&A content.
    """
    try:
        data = json.loads(raw_json)
        # Assumes the content is the value of the 'mda' key
        content = data.get("mda")
        return str(content) if content else None
    except (json.JSONDecodeError, TypeError):
        return None