"""
Helper functions for parsing filenames and business profile data.
"""
import os
import re
import json

def parse_filename(blob_name: str) -> str | None:
    """
    Returns ticker if filename matches the expected pattern,
    otherwise None. Example: 'AAL_business_profile.json'
    """
    # Regex to capture TICKER from the base filename
    pattern = re.compile(r"([A-Z]+)_business_profile\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return match.group(1) if match else None

def read_business_profile_data(raw_content: str) -> str | None:
    """
    Parses raw JSON text to extract the business profile content.
    """
    try:
        data = json.loads(raw_content)
        # Assuming the business profile is the value of the 'business' key
        content = data.get("business")

        if not content:
            return None

        return str(content)

    except (json.JSONDecodeError, TypeError, ValueError, IndexError):
        # If JSON is malformed or data types are wrong, return None
        return None