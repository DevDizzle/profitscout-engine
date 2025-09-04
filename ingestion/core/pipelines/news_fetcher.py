import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import json
import os
import re

from .. import config, gcs
from ..clients import vertex_ai  # <-- CORRECTED IMPORT
from ..clients.ploygon_client import PolygonClient # <-- CORRECTED IMPORT

# ... (the rest of the file is the same as what you provided)
PROFILE_INPUT_PREFIX = "sec-business/"
NEWS_OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
QUERY_CACHE_PREFIX = config.PREFIXES["news_fetcher"]["query_cache"]
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

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

def map_polygon_news(article: dict) -> dict:
    return {
        "title": article.get("title"),
        "publishedDate": article.get("published"),
        "image": article.get("images", [None])[0],
        "site": article.get("author"),
        "text": article.get("teaser") or article.get("body", "")[:500],
        "url": article.get("url"),
    }

def fetch_and_save_headlines(ticker: str, topics_str: str):
    """Fetches, filters, merges, saves, and cleans up news headlines for a given ticker."""
    if not POLYGON_API_KEY:
        logging.critical("POLYGON_API_KEY environment variable not set. Aborting news fetch.")
        return None

    client = PolygonClient(POLYGON_API_KEY)
    today = datetime.date.today()
    from_date = today - datetime.timedelta(days=7)
    today_str = today.strftime("%Y-%m-%d")
    from_date_str = from_date.strftime("%Y-%m-%d")
    
    try:
        stock_news_raw = client.fetch_news(ticker=ticker, from_date=from_date_str, to_date=today_str, limit_per_page=10, paginate=False)
        stock_news = [map_polygon_news(article) for article in stock_news_raw]
    except Exception:
        stock_news = []

    press_news = []

    try:
        general_news_raw = client.fetch_news(from_date=from_date_str, to_date=today_str, limit_per_page=10, paginate=False, topics_str=topics_str)
        general_news = [map_polygon_news(article) for article in general_news_raw]
    except Exception:
        general_news = []
        
    def is_within_range(article_date_str: str) -> bool:
        if not article_date_str: return False
        try:
            date_part = article_date_str.split('T')[0] if 'T' in article_date_str else article_date_str.split(' ')[0]
            article_date = datetime.date.fromisoformat(date_part)
            return from_date <= article_date <= today
        except (ValueError, TypeError):
            return False

    stock_news = [a for a in stock_news if is_within_range(a.get('publishedDate'))]
    press_news = [a for a in press_news if is_within_range(a.get('date') or a.get('publishedDate'))]
    general_news = [a for a in general_news if is_within_range(a.get('publishedDate'))]

    all_news = (stock_news or []) + (press_news or []) + general_news
    merged = {(a["title"], a.get("site") or a.get("author")): a for a in all_news if a.get("publishedDate")}
    
    sorted_news = sorted(list(merged.values()), key=lambda x: x['publishedDate'], reverse=True)
    headlines = sorted_news[:1]
    
    output_path = f"{NEWS_OUTPUT_PREFIX}{ticker}_{today_str}.json"
    gcs.write_text(config.GCS_BUCKET_NAME, output_path, json.dumps(headlines, indent=2), "application/json")
    logging.info(f"[{ticker}] Saved {len(headlines)} filtered headlines to {output_path}")
    
    gcs.cleanup_old_files(config.GCS_BUCKET_NAME, NEWS_OUTPUT_PREFIX, ticker, output_path)
    
    return output_path

def process_profile_blob(blob_name: str):
    """Orchestrates the process for a single company profile."""
    match = re.search(r'sec-business/([A-Z.]+)_', blob_name)
    if not match: return None
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
    if not POLYGON_API_KEY:
        logging.critical("POLYGON_API_KEY environment variable not set. Aborting news_fetcher.")
        return

    logging.info("--- Starting News Fetcher Pipeline ---")
    
    profile_blobs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=PROFILE_INPUT_PREFIX)
    if not profile_blobs:
        logging.warning("No business profiles found to process for news.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_profile_blob, blob) for blob in profile_blobs]
        count = sum(1 for future in as_completed(futures) if future.result())

    logging.info(f"--- News Fetcher Pipeline Finished. Fetched news for {count} tickers. ---")