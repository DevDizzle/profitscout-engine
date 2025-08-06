"""
Helper functions for parsing filenames and ratios data.
"""
import os
import re

def parse_filename(blob_name: str) -> tuple[str | None, str | None]:
    """
    Returns (ticker, date) if filename matches the expected pattern,
    otherwise (None, None). Example: 'AAL_2025-06-30.json'
    """
    # Regex to capture TICKER and YYYY-MM-DD from the base filename
    pattern = re.compile(r"([A-Z]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)

def read_ratios_data(raw_content: str) -> str | None:
    """
    Passes through the raw text content from the file.
    """
    if raw_content and isinstance(raw_content, str):
        return raw_content
    return None