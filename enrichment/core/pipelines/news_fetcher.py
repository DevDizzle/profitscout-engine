import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import json
import urllib.parse
import requests
import re
import os

from .. import config, gcs
from ..clients import vertex_ai

PROFILE_INPUT_PREFIX = "sec-business/"
NEWS_OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
QUERY_CACHE_PREFIX = config.PREFIXES["news_fetcher"]["query_cache"]
FMP_API_KEY = os.getenv("FMP_API_KEY") 

def get_or_create_news_query(ticker: str, business_profile: str) -> str:
    """
    Checks for a cached query. If not found, generates a new one and saves it.
    """
    query_cache_path = f"{QUERY_CACHE_PREFIX}{ticker}_query.txt"
    
    cached_query = gcs.read_blob(config.GCS_BUCKET_NAME, query_cache_path)
    if cached_query:
        logging.info(f"[{ticker}] Found cached news query.")
        return cached_query

    logging.info(f"[{ticker}] No cached query found. Generating a new one via Vertex AI.")
    prompt = f"""
You are a financial news analyst bot. Your sole task is to read a company's business profile and generate a list of key topics for a news search.
- Your entire output must be ONLY a comma-separated list of these key topics.
- Do not include any other text, explanations, or markdown.
- Do not include the company's name in the topics.

Business Profile:
{business_profile}
"""
    new_query = vertex_ai.generate(prompt)
    if not new_query:
        return ""

    gcs.write_text(config.GCS_BUCKET_NAME, query_cache_path, new_query)
    logging.info(f"[{ticker}] Saved new query to cache: {query_cache_path}")
    
    return new_query

def fetch_and_save_headlines(ticker: str, topics_str: str):
    """Fetches, filters, merges, saves, and cleans up news headlines for a given ticker."""
    today = datetime.date.today()
    from_date = today - datetime.timedelta(days=7)
    today_str = today.strftime("%Y-%m-%d")
    from_date_str = from_date.strftime("%Y-%m-%d")
    
    # --- Part 1: Fetching ---
    url_stock = f"https://financialmodelingprep.com/api/v3/stock_news?tickers={ticker}&from={from_date_str}&to={today_str}&limit=100&page=0&apikey={FMP_API_KEY}"
    try:
        stock_news = requests.get(url_stock, timeout=30).json()
    except Exception:
        stock_news = []

    url_press = f"https://financialmodelingprep.com/api/v3/press-releases/{ticker}?from={from_date_str}&to={today_str}&apikey={FMP_API_KEY}"
    try:
        press_news = requests.get(url_press, timeout=30).json()
    except Exception:
        press_news = []

    url_general = f"https://financialmodelingprep.com/api/v4/general_news?from={from_date_str}&to={today_str}&page=0&apikey={FMP_API_KEY}"
    try:
        general_news = requests.get(url_general, timeout=30).json()
    except Exception:
        general_news = []
        
    # --- Part 2: Client-Side Date Filtering ---
    def is_within_range(article_date_str: str) -> bool:
        if not article_date_str:
            return False
        try:
            # Extract just the 'YYYY-MM-DD' part and convert to a date object
            article_date = datetime.datetime.fromisoformat(article_date_str.split(' ')[0]).date()
            return from_date <= article_date <= today
        except (ValueError, TypeError):
            return False

    stock_news = [article for article in stock_news if is_within_range(article.get('publishedDate'))]
    # Handle different possible date fields for press releases
    press_news = [article for article in press_news if is_within_range(article.get('date') or article.get('publishedDate'))]
    general_news = [article for article in general_news if is_within_range(article.get('publishedDate'))]

    # --- Part 3: Topic Filtering and Merging ---
    filtered_general = []
    if topics_str and general_news:
        topics = [topic.strip().lower() for topic in topics_str.split(',')]
        filtered_general = [
            article for article in general_news
            if any(topic in (str(article.get('title', '')) + str(article.get('text', ''))).lower() for topic in topics)
        ]

    all_news = (stock_news or []) + (press_news or []) + filtered_general
    merged = {(article["title"], article.get("site") or article.get("source")): article for article in all_news}
    headlines = list(merged.values())
    
    # --- Part 4: Saving and Cleaning Up ---
    output_path = f"{NEWS_OUTPUT_PREFIX}{ticker}_{today_str}.json"
    gcs.write_text(config.GCS_BUCKET_NAME, output_path, json.dumps(headlines, indent=2), "application/json")
    logging.info(f"[{ticker}] Saved {len(headlines)} filtered headlines to {output_path}")
    
    gcs.cleanup_old_files(config.GCS_BUCKET_NAME, NEWS_OUTPUT_PREFIX, ticker, output_path)
    
    return output_path

def process_profile_blob(blob_name: str):
    """Orchestrates the process for a single company profile."""
    match = re.search(r'sec-business/([A-Z.]+)_', blob_name)
    if not match:
        return None
    ticker = match.group(1)

    profile_content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    if not profile_content:
        logging.warning(f"Could not read business profile for {ticker}.")
        return None

    query_topics = get_or_create_news_query(ticker, profile_content)
    if not query_topics:
        logging.warning(f"Could not get or create news query topics for {ticker}.")
        return None
        
    return fetch_and_save_headlines(ticker, query_topics)

def run_pipeline():
    """Main pipeline execution logic."""
    if not FMP_API_KEY:
        logging.critical("FMP_API_KEY environment variable not set. Aborting news_fetcher.")
        return

    logging.info("--- Starting News Fetcher Pipeline (with Caching and Client-Side Filtering) ---")
    
    profile_blobs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=PROFILE_INPUT_PREFIX)
    if not profile_blobs:
        logging.warning("No business profiles found to process for news.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_profile_blob, blob) for blob in profile_blobs]
        count = sum(1 for future in as_completed(futures) if future.result())

    logging.info(f"--- News Fetcher Pipeline Finished. Fetched news for {count} tickers. ---")