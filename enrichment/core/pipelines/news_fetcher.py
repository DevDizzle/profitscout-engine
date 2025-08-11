import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import json
import urllib.parse
import requests

from .. import config, gcs
from ..clients import vertex_ai # We need this for the query generation
from . import utils # A new shared utils file might be needed

# --- Configuration ---
PROFILE_INPUT_PREFIX = "sec-business/"
NEWS_OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"] # This is "headline-news/"
FMP_API_KEY = "YOUR_FMP_API_KEY_SECRET" # Make sure this is available, perhaps via a shared config or secret manager
HEADLINE_LIMIT = 25

# --- Logic adapted from profile_summarizer ---
def generate_news_query(business_profile: str) -> str:
    """Generates the boolean search query from a business profile."""
    prompt = f"""
You are a financial news analyst bot. Your sole task is to read a company's business profile and generate a list of key topics for a news search.
- Your entire output must be ONLY a comma-separated list of these key topics.
- Do not include any other text, explanations, or markdown.
- Do not include the company's name in the topics.

Business Profile:
{business_profile}
"""
    query = vertex_ai.generate(prompt)
    return f'"{query}"' if query else ""


# --- Logic adapted from news_fetcher ---
def fetch_and_save_headlines(ticker: str, query_str: str):
    """Fetches, merges, and saves news headlines for a given ticker."""
    today = datetime.date.today().strftime("%Y-%m-%d")
    
    # 1. Company-Tagged News
    url_stock = f"https://financialmodelingprep.com/api/v3/stock_news?tickers={ticker}&from={today}&to={today}&limit={HEADLINE_LIMIT}&apikey={FMP_API_KEY}"
    try:
        stock_news = requests.get(url_stock, timeout=30).json()
    except Exception:
        stock_news = []

    # 2. Keyword-Based News
    url_macro = f"https://financialmodelingprep.com/api/v3/search_stock_news?query={urllib.parse.quote(query_str)}&from={today}&to={today}&limit={HEADLINE_LIMIT}&apikey={FMP_API_KEY}"
    try:
        macro_news = requests.get(url_macro, timeout=30).json()
    except Exception:
        macro_news = []

    # Merge and format
    merged = {(article["title"], article["site"]): article for article in (stock_news or []) + (macro_news or [])}
    headlines = list(merged.values())
    
    # Save with the correct name for the news_analyzer
    output_path = f"{NEWS_OUTPUT_PREFIX}{ticker}_{today}.json"
    gcs.write_text(config.GCS_BUCKET_NAME, output_path, json.dumps(headlines, indent=2), "application/json")
    logging.info(f"[{ticker}] Saved {len(headlines)} headlines to {output_path}")
    return output_path

def process_profile_blob(blob_name: str):
    """Orchestrates the process for a single company profile."""
    # This pattern should correctly extract the ticker, e.g., 'AAPL' from 'sec-business/AAPL_business_profile.json'
    ticker_match = re.search(r'/([A-Z]+)_business_profile\.json$', blob_name)
    if not ticker_match:
        return None
    ticker = ticker_match.group(1)

    profile_content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    if not profile_content:
        return None

    query = generate_news_query(profile_content)
    if not query:
        logging.warning(f"Could not generate news query for {ticker}.")
        return None
        
    return fetch_and_save_headlines(ticker, query)

def run_pipeline():
    """Main pipeline execution logic."""
    logging.info("--- Starting News Fetcher Pipeline ---")
    
    profile_blobs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=PROFILE_INPUT_PREFIX)
    if not profile_blobs:
        logging.warning("No business profiles found to process.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_profile_blob, blob) for blob in profile_blobs]
        count = sum(1 for future in as_completed(futures) if future.result())

    logging.info(f"--- News Fetcher Pipeline Finished. Fetched news for {count} tickers. ---")