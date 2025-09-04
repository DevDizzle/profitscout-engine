import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import json
import os
import re

from .. import config, gcs
from ..clients.polygon_client import PolygonClient

PROFILE_INPUT_PREFIX = "sec-business/"
NEWS_OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

def map_polygon_news(article: dict) -> dict:
    return {
        "title": article.get("title"),
        "publishedDate": article.get("published"),
        "image": article.get("images", [None])[0],
        "site": article.get("author"),
        "text": article.get("teaser") or article.get("body", "")[:500],
        "url": article.get("url"),
    }

def fetch_and_save_headlines(ticker: str):
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
        
    def is_within_range(article_date_str: str) -> bool:
        if not article_date_str: return False
        try:
            date_part = article_date_str.split('T')[0] if 'T' in article_date_str else article_date_str.split(' ')[0]
            article_date = datetime.date.fromisoformat(date_part)
            return from_date <= article_date <= today
        except (ValueError, TypeError):
            return False

    stock_news = [a for a in stock_news if is_within_range(a.get('publishedDate'))]

    all_news = stock_news or []
    merged = {(a["title"], a.get("site") or a.get("author")): a for a in all_news if a.get("publishedDate")}
    
    sorted_news = sorted(list(merged.values()), key=lambda x: x['publishedDate'], reverse=True)
    headlines = sorted_news[:5]
    
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
        
    return fetch_and_save_headlines(ticker)

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