from google.cloud import storage
import datetime, json, urllib.parse, requests, logging
from . import config

def fetch_and_save_headlines(ticker: str, query_str: str, api_key: str, bucket_name: str, output_prefix: str) -> str:
    """
    Fetch today’s headlines for `ticker` using two methods, merge them,
    and write the result to GCS in a flattened file format.
    """
    today = datetime.date.today().strftime("%Y-%m-%d")
    limit = config.HEADLINE_LIMIT

    # --- 1. Company-Tagged News (Specific) ---
    url_stock = (
        "https://financialmodelingprep.com/api/v3/stock_news"
        f"?tickers={ticker}"
        f"&from={today}&to={today}&limit={limit}&apikey={api_key}"
    )
    try:
        stock_news_response = requests.get(url_stock, timeout=20)
        stock_news_response.raise_for_status()
        stock_news = stock_news_response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch stock news for {ticker}: {e}")
        stock_news = []

    # --- 2. Keyword-Based News (Broader Context) ---
    encoded_q = urllib.parse.quote(query_str)
    url_macro = (
        "https://financialmodelingprep.com/api/v3/search_stock_news"
        f"?query={encoded_q}"
        f"&from={today}&to={today}&limit={limit}&apikey={api_key}"
    )
    try:
        macro_news_response = requests.get(url_macro, timeout=20)
        macro_news_response.raise_for_status()
        macro_news = macro_news_response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch macro news with query '{query_str}': {e}")
        macro_news = []

    # --- Merge, Deduplicate, and Save ---
    merged = { (article["title"], article["site"]): article for article in (stock_news or []) + (macro_news or []) }
    headlines = list(merged.values())

    if not headlines:
        logging.warning(f"No headlines found for {ticker} with query '{query_str}'. Output will be empty.")

    # --- Write the final list to GCS ---
    client = storage.Client()
    
    # --- CHANGE: Flatten the output filename structure ---
    blob_path = f"{output_prefix}{ticker}_{today}.json"
    
    blob = client.bucket(bucket_name).blob(blob_path)
    blob.upload_from_string(json.dumps(headlines, indent=2),
                            content_type="application/json")

    return f"gs://{bucket_name}/{blob_path}"