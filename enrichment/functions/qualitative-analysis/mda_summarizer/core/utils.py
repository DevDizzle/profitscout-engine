"""
Helper functions for parsing MD&A filenames and data.
"""
import json
import os
import re
from datetime import datetime

def parse_filename(blob_name: str) -> tuple[str | None, str | None, str | None, int | None, int | None]:
    """
    Parses filenames like 'AAL_2025-06-30.json' by inferring the filing type.
    Returns (ticker, date_str, filing_type, year, quarter).
    """
    # This regex matches the new, simpler filename format (e.g., 'WTRG_2025-06-30.json')
    pattern = re.compile(r"([A-Z]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    
    if not match:
        return None, None, None, None, None

    ticker, date_str = match.groups()
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        quarter = (date_obj.month - 1) // 3 + 1
        year = date_obj.year

        # Infer filing type based on the quarter-end month.
        # SEC filings for Q1, Q2, and Q3 are in a 10-Q. Year-end filings are in a 10-K.
        if date_obj.month in [3, 6, 9]:
            filing_type = "10-Q"
        else:
            filing_type = "10-K"
            
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