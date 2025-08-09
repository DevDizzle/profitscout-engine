"""
Helper functions for parsing MD&A summary filenames and data.
"""
import os
import re

def parse_filename(blob_name: str) -> tuple[str | None, str | None]:
    """
    Parses filenames like 'AAL_2025-06-30.txt'.
    Returns (ticker, date_str).
    """
    # --- MODIFIED: Regex to handle the new '.txt' format without filing type ---
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.txt$")
    match = pattern.search(os.path.basename(blob_name))
    if not match:
        return None, None
    
    ticker, date_str = match.groups()
    return ticker, date_str

def read_mda_summary_data(raw_content: str) -> str | None:
    """
    Passes through the raw text content from the summary file.
    """
    if raw_content and isinstance(raw_content, str):
        return raw_content
    return None