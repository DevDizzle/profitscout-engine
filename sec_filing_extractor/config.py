import os

# --- GCP Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- API Keys ---
SEC_API_KEY_SECRET = os.getenv("SEC_API_KEY_SECRET", "SEC_API_KEY")

# --- Job Parameters ---
TICKER_LIST_PATH = "tickerlist.txt"
MAX_WORKERS = 4 # Lowered due to multiple extractions per filing

# --- GCS Output Folders ---
BUSINESS_FOLDER = "sec-business/"
MDA_FOLDER = "sec-mda/"
RISK_FOLDER = "sec-risk/"

# --- SEC Section Mapping ---
# Maps the section we want to the item number required by the sec-api
# 'business' is only defined for annual reports (10-K, etc.)
SECTION_MAP = {
    "10-K": {
        "business": "1",
        "mda": "7",
        "risk": "1A",
    },
    "10-KT": { # Transitional 10-K
        "business": "1",
        "mda": "7",
        "risk": "1A",
    },
    "20-F": { # Foreign annual report
        "business": "item4",
    },
    "40-F": { # Canadian annual report
        "business": "1",
    },
    "10-Q": {
        "mda": "part1item2",
        "risk": "part2item1a",
    },
    "10-QT": { # Transitional 10-Q
        "mda": "part1item2",
        "risk": "part2item1a",
    }
}