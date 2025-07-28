import requests
import os
from google.cloud import storage
from math import ceil

# Configuration
FMP_API_KEY = 'your_fmp_api_key_here'  # Replace with your premium key
GCS_BUCKET_NAME = 'your_gcs_bucket_name'  # e.g., 'my-stock-app-bucket'
BATCH_SIZE = 50  # Adjust based on your rate limits; FMP supports large comma-separated lists

# Load tickers (example: from a file; one per line)
with open('tickers.txt', 'r') as f:
    tickers = [line.strip() for line in f if line.strip()]

# Or hardcode a small list for testing: tickers = ['AAPL', 'MSFT', 'GOOGL']

# Function to upload image to GCS and get public URI
def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    blob.make_public()  # Makes the object publicly accessible
    return blob.public_url

# Batch process tickers
num_batches = ceil(len(tickers) / BATCH_SIZE)
uri_map = {}  # To store ticker: URI pairs

for i in range(num_batches):
    batch_start = i * BATCH_SIZE
    batch_end = batch_start + BATCH_SIZE
    batch_tickers = tickers[batch_start:batch_end]
    tickers_str = ','.join(batch_tickers)
    
    # Fetch profiles from FMP
    api_url = f"https://financialmodelingprep.com/api/v3/profile/{tickers_str}?apikey={FMP_API_KEY}"
    response = requests.get(api_url)
    
    if response.status_code != 200:
        print(f"API error for batch {i+1}: {response.text}")
        continue
    
    data = response.json()
    
    for item in data:
        ticker = item.get('symbol')
        logo_url = item.get('image')
        
        if not logo_url:
            print(f"No logo for {ticker}")
            continue
        
        # Download image
        img_response = requests.get(logo_url)
        if img_response.status_code != 200:
            print(f"Failed to download logo for {ticker}")
            continue
        
        # Determine file extension from URL (usually .png)
        ext = logo_url.split('.')[-1] if '.' in logo_url else 'png'
        local_file = f"{ticker}.{ext}"
        with open(local_file, 'wb') as f:
            f.write(img_response.content)
        
        # Upload to GCS (e.g., in a 'logos' folder)
        blob_name = f"logos/{ticker}.{ext}"
        gcs_uri = upload_to_gcs(GCS_BUCKET_NAME, local_file, blob_name)
        uri_map[ticker] = gcs_uri
        print(f"Uploaded {ticker}: {gcs_uri}")
        
        # Clean up local file
        os.remove(local_file)

# Optional: Save URI map to a file (e.g., for your database/website)
with open('ticker_uris.json', 'w') as f:
    import json
    json.dump(uri_map, f, indent=4)

print("Process complete!")