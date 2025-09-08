# ingestion/core/pipelines/news_fetcher.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import json
import os
from google.cloud import storage
from bs4 import BeautifulSoup  # <-- ADD THIS IMPORT
from .. import config, gcs
from ..clients.polygon_client import PolygonClient

NEWS_OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

def map_polygon_news(article: dict) -> dict:
    """
    Safely maps a Polygon news article and cleans HTML from the text.
    """
    # Get the raw HTML content from the 'body' or 'teaser'
    html_text = article.get("body") or article.get("teaser", "")
    
    # Use BeautifulSoup to parse the HTML and get only the plain text
    soup = BeautifulSoup(html_text, "html.parser")
    plain_text = soup.get_text(separator=" ", strip=True)

    return {
        "title": article.get("title"),
        "publishedDate": article.get("published"),
        "image": None,
        "site": article.get("author"),
        "text": plain_text,  # Use the cleaned, plain text
        "url": article.get("url"),
    }

def fetch_and_save_headlines(ticker: str, storage_client: storage.Client):
    """Fetches up to 4 recent stock-specific and macro news articles for a ticker."""
    if not POLYGON_API_KEY:
        logging.critical("POLYGON_API_KEY not set. Aborting news fetch for %s.", ticker)
        return None

    client = PolygonClient(POLYGON_API_KEY)
    today = datetime.date.today()
    from_date = today - datetime.timedelta(days=7)
    from_date_str = from_date.strftime("%Y-%m-%d")
    to_date_str = today.strftime("%Y-%m-%d")
    
    # Fetch stock-specific news (up to 4 recent)
    try:
        stock_news_raw = client.fetch_news(
            ticker=ticker, 
            from_date=from_date_str, 
            to_date=to_date_str, 
            limit_per_page=4
        )
        stock_news = [map_polygon_news(article) for article in stock_news_raw]
        logging.info(f"[{ticker}] Fetched {len(stock_news)} stock-specific articles.")
    except Exception as e:
        logging.error(f"[{ticker}] Failed to fetch stock news: {e}")
        stock_news = []
    
    # Fetch macro news (up to 4 recent, filtered for economy/markets)
    try:
        macro_news_raw = client.fetch_news(
            from_date=from_date_str, 
            to_date=to_date_str, 
            limit_per_page=4,
            topics_str="economy,markets,federal reserve,inflation,gdp,geopolitics"  # Fixed macro filters
        )
        macro_news = [map_polygon_news(article) for article in macro_news_raw]
        logging.info(f"[{ticker}] Fetched {len(macro_news)} macro articles.")
    except Exception as e:
        logging.error(f"[{ticker}] Failed to fetch macro news: {e}")
        macro_news = []
    
    # Save as structured dict (allows downstream separate or combined processing)
    headlines = {
        "stock_news": sorted(stock_news, key=lambda x: x.get('publishedDate', ''), reverse=True),
        "macro_news": sorted(macro_news, key=lambda x: x.get('publishedDate', ''), reverse=True)
    }
    
    output_path = f"{NEWS_OUTPUT_PREFIX}{ticker}_{today.strftime('%Y-%m-%d')}.json"
    gcs.upload_json_to_gcs(storage_client, headlines, output_path)
    gcs.cleanup_old_files(storage_client, NEWS_OUTPUT_PREFIX, ticker, output_path)
    
    if not headlines["stock_news"] and not headlines["macro_news"]:
        logging.info(f"[{ticker}] No recent news found. Saved empty file.")
        return None

    return output_path

def run_pipeline():
    """Main pipeline execution logic."""
    if not POLYGON_API_KEY:
        logging.critical("POLYGON_API_KEY not set. Aborting news_fetcher.")
        return

    logging.info("--- Starting News Fetcher Pipeline (with HTML cleaning) ---")
    
    storage_client = storage.Client()
    tickers = gcs.get_tickers(storage_client)
    if not tickers:
        logging.warning("No tickers found in tickerlist.txt.")
        return

    logging.info(f"Processing news for {len(tickers)} tickers.")
    max_workers = config.MAX_WORKERS_TIERING.get("news_fetcher", 8)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_and_save_headlines, ticker, storage_client): ticker for ticker in tickers}
        processed_count = len(futures)

    logging.info(f"--- News Fetcher Pipeline Finished. Processed {processed_count} tickers. ---")
