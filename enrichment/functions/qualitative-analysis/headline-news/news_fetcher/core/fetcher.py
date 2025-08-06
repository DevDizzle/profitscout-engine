from google.cloud import storage
import datetime, json, urllib.parse, requests
from . import config # Import the config module

def fetch_and_save_headlines(ticker: str, query_str: str, api_key: str, bucket_name: str, output_prefix: str) -> str:
    """
    Fetch today’s headlines for `ticker` using:
      1) /stock_news?tickers=...      (company-tagged)
      2) /search_stock_news?query=...   (macro / sector)
    Merge, dedupe, and write one JSON array to GCS.

    Returns the GCS URI that was written.
    """
    today = datetime.date.today().strftime("%Y-%m-%d")
    # --- FIX: Use the new configurable limit from the config file ---
    limit = config.HEADLINE_LIMIT

    # --- company-tagged feed -------------------------------------------------
    url_stock = (
        "https://financialmodelingprep.com/api/v3/stock_news"
        f"?tickers={ticker}"
        f"&from={today}&to={today}&limit={limit}&apikey={api_key}"
    )
    stock_news = requests.get(url_stock, timeout=20).json()

    # --- macro / sector feed (Boolean query, no ticker) ----------------------
    encoded_q  = urllib.parse.quote(query_str)
    url_macro = (
        "https://financialmodelingprep.com/api/v3/search_stock_news"
        f"?query={encoded_q}"
        f"&from={today}&to={today}&limit={limit}&apikey={api_key}"
    )
    macro_news = requests.get(url_macro, timeout=20).json()

    # --- merge + dedupe on (title, site) ------------------------------------
    merged = { (a["title"], a["site"]): a for a in (stock_news or []) + (macro_news or []) }
    headlines = list(merged.values())

    # --- write to GCS --------------------------------------------------------
    client = storage.Client()
    blob_path = f"{output_prefix}{ticker}/{today}.json"  # e.g. headline-news/AAL/2025-08-05.json
    blob = client.bucket(bucket_name).blob(blob_path)
    blob.upload_from_string(json.dumps(headlines, indent=2),
                            content_type="application/json")

    return f"gs://{bucket_name}/{blob_path}"