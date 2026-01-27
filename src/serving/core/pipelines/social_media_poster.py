import logging
import json
import time
from datetime import date
from google.cloud import bigquery
from google.cloud import firestore
from .. import config
from ..clients import vertex_ai
from ..clients.x_client import XClient
from ..gcs import read_blob

def run_pipeline():
    logging.info("Starting Social Media Poster pipeline...")
    
    # 1. Initialize clients
    bq_client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    db = firestore.Client(project=config.DESTINATION_PROJECT_ID)
    x_client = XClient()
    
    if not x_client.client:
        logging.error("X Client not available. Aborting.")
        return

    # 2. Fetch Winners
    table_id = config.SOURCE_WINNERS_DASHBOARD_TABLE_ID
    today = date.today().isoformat()
    
    # We query for tickers that have a high score. 
    # Adjust logic if specific criteria are needed beyond sorting by score.
    query = f"""
        SELECT ticker, weighted_score, setup_quality_signal 
        FROM `{table_id}`
        WHERE run_date = CAST(CURRENT_DATE() AS STRING)
        ORDER BY weighted_score DESC
        LIMIT 10
    """
    
    try:
        query_job = bq_client.query(query)
        winners = [dict(row) for row in query_job]
    except Exception as e:
        logging.error(f"Failed to query winners: {e}")
        return

    if not winners:
        logging.info("No winners found for today.")
        return

    logging.info(f"Found {len(winners)} potential winners to post.")

    # 3. Process Winners
    posts_count = 0
    max_posts = 15
    
    collection_ref = db.collection(config.SOCIAL_MEDIA_HISTORY_COLLECTION)

    for winner in winners:
        if posts_count >= max_posts:
            logging.info("Max posts limit reached.")
            break

        ticker = winner['ticker']
        doc_id = f"{ticker}_{today}"
        
        # Check if already posted
        doc_ref = collection_ref.document(doc_id)
        if doc_ref.get().exists:
            logging.info(f"Skipping {ticker}: Already posted today.")
            continue

        # Fetch Page Content
        # Filename format: {ticker}_page_{date}.json
        # GCS path: config.GCS_BUCKET_NAME / config.PAGE_JSON_PREFIX / ...
        blob_name = f"{config.PAGE_JSON_PREFIX}{ticker}_page_{today}.json"
        content_str = read_blob(config.GCS_BUCKET_NAME, blob_name)
        
        if not content_str:
            logging.warning(f"Page content not found for {ticker} at {blob_name}. Skipping.")
            continue
            
        try:
            page_data = json.loads(content_str)
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON for {ticker}. Skipping.")
            continue

        # Extract relevant info
        seo_title = page_data.get('seo', {}).get('title', '')
        analyst_brief = page_data.get('analystBrief', '')
        trade_setup = page_data.get('tradeSetup', '')
        
        # Generate Tweet
        prompt = f"""
        You are a professional financial analyst for GammaRips. Write a catchy, professional "FinTwit" style tweet for the stock ${ticker}.
        
        Context:
        - Title: {seo_title}
        - Analyst Brief: {analyst_brief}
        - Trade Setup: {trade_setup}
        
        Requirements:
        - Start with the Cashtag ${ticker} and a relevant emoji.
        - Highlight the key level or direction (Call Wall, Support, etc.).
        - Keep it under 260 characters.
        - Include the link: https://gammarips.com/{ticker}
        - Do NOT use hashtags other than the Cashtag.
        - Tone: Confident, actionable, data-driven.
        """
        
        try:
            tweet_text = vertex_ai.generate(prompt)
            # Basic cleanup if model includes quotes
            tweet_text = tweet_text.strip('"').strip("'")
            
            # Post to X
            logging.info(f"Posting tweet for {ticker}...")
            tweet_id = x_client.post_tweet(tweet_text)
            
            if tweet_id:
                # Log to Firestore
                doc_ref.set({
                    'ticker': ticker,
                    'date': today,
                    'tweet_id': tweet_id,
                    'text': tweet_text,
                    'timestamp': firestore.SERVER_TIMESTAMP
                })
                posts_count += 1
                logging.info(f"Successfully posted for {ticker}.")
                
                # Rate limit safety
                time.sleep(30) 
            else:
                logging.error(f"Failed to post for {ticker}.")

        except Exception as e:
            logging.error(f"Error processing {ticker}: {e}")
            continue

    logging.info("Social Media Poster pipeline finished.")
