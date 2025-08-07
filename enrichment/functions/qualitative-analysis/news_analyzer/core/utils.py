"""
Helper functions for parsing news filenames and data.
"""
import os
import re

def parse_filename(blob_name: str) -> tuple[str | None, str | None]:
    """
    Parses filenames like 'AAPL_2025-08-06.json'.
    Returns (ticker, date_str).
    """
    pattern = re.compile(r"([A-Z]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    if not match:
        return None, None
    
    ticker, date_str = match.groups()
    return ticker, date_str

def read_news_data(raw_content: str) -> str | None:
    """
    Passes through the raw text content from the file.
    This function is designed to be robust for the prompt.
    """
    if raw_content and isinstance(raw_content, str):
        return raw_content
    return None