"""
Helper functions for parsing filenames and technicals data.
"""
import os
import re

def parse_filename(blob_name: str) -> tuple[str | None, str | None]:
    """
    Returns (ticker, 'technicals') if filename matches the expected pattern,
    otherwise (None, None). Example: 'AAPL_technicals.json'
    """
    # Regex to capture TICKER from 'TICKER_technicals.json'
    pattern = re.compile(r"([A-Z]+)_technicals\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), "technicals") if match else (None, None)

def read_technicals_data(raw_content: str) -> str | None:
    """
    Passes through the raw text content from the file.
    """
    if raw_content and isinstance(raw_content, str):
        return raw_content
    return None