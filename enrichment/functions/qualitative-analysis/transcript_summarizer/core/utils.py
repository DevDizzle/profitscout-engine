"""
Helper functions for parsing filenames and transcript data.
"""
import json
import os
import re

def parse_filename(blob_name: str) -> tuple[str | None, str | None]:
    """
    Returns (ticker, date) if filename matches the expected pattern,
    otherwise (None, None). Example: 'AAPL_2023-10-27.json'
    """
    # Regex to capture TICKER and YYYY-MM-DD from the base filename
    pattern = re.compile(r"([A-Z]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)

def read_transcript_data(raw_json: str) -> tuple[str | None, int | None, int | None]:
    """
    Parses raw JSON text to extract the transcript content, year, and quarter.
    
    Returns:
        A tuple containing (content, year, quarter).
        Returns (None, None, None) if essential data cannot be found.
    """
    try:
        data = json.loads(raw_json)
        # The FMP API returns a list with one object
        if isinstance(data, list) and data:
            data = data[0]

        content = data.get("content")
        year = data.get("year")
        quarter = data.get("quarter")

        if not all([content, year, quarter]):
            return None, None, None
            
        return str(content), int(year), int(quarter)

    except (json.JSONDecodeError, TypeError, ValueError, IndexError):
        # If JSON is malformed or data types are wrong, return None        
        return None, None, None
