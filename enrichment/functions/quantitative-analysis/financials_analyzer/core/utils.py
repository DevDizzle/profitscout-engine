"""
Helper functions for parsing filenames and financial data.
"""
import json
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

def read_financial_data(raw_content: str) -> str | None:
    """
    Returns the raw text content from the file.

    This function is designed to be robust and will pass through any non-empty
    string, whether it is valid JSON or plain text.
    
    Returns:
        The raw string content if it's not empty, otherwise None.
    """
    if raw_content and isinstance(raw_content, str):
        return raw_content
    return None