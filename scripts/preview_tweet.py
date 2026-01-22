import logging
import sys
import os
import json
import re

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from serving.core.clients import vertex_ai
from serving.core.gcs import read_blob
from serving.core import config

# Setup logging
logging.basicConfig(level=logging.INFO)

def preview_tweet():
    print("--- GENERATING PREVIEW TWEET FROM GCS ---")
    
    ticker = "ABBV"
    date_str = "2026-01-19" # As requested by user
    bucket_name = config.GCS_BUCKET_NAME
    blob_name = f"pages/{ticker}_page_{date_str}.json"
    
    print(f"Fetching: gs://{bucket_name}/{blob_name}")
    
    content_str = read_blob(bucket_name, blob_name)
    
    if not content_str:
        print(f"ERROR: Could not read blob: {blob_name}")
        return

    try:
        page_data = json.loads(content_str)
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse JSON: {e}")
        return
    
    seo_title = page_data.get('seo', {}).get('title', 'No Title')
    
    # Extract text from Analyst Brief (strip HTML for prompt context)
    raw_brief = page_data.get('analystBrief', {}).get('content', '')
    clean_brief = re.sub('<[^<]+?>', '', raw_brief)
    
    trade_setup = str(page_data.get('tradeSetup', {}))
    
    prompt = f"""
    You are a professional financial analyst for GammaRips. Write a catchy, professional "FinTwit" style tweet for the stock ${ticker}.
    
    Context:
    - Title: {seo_title}
    - Analyst Brief: {clean_brief}
    - Trade Setup: {trade_setup}
    
    Requirements:
    - Start with the Cashtag ${ticker} and a relevant emoji.
    - Highlight the key level or direction (Call Wall, Support, etc.).
    - Keep it under 280 characters.
    - Include the link: https://gammarips.com/{ticker}
    - Do NOT use hashtags other than the Cashtag.
    - Tone: Confident, actionable, data-driven.
    """
    
    print(f"\n[Prompt Sent to Model]:\n{prompt}\n")
    
    try:
        tweet_text = vertex_ai.generate(prompt)
        tweet_text = tweet_text.strip('"').strip("'")
        
        print(f"\n[Generated Tweet ({len(tweet_text)} chars)]:")
        print("-" * 40)
        print(tweet_text)
        print("-" * 40)
        
    except Exception as e:
        print(f"Error generating tweet: {e}")

if __name__ == "__main__":
    preview_tweet()
