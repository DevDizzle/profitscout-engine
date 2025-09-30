# deploy_all.sh

# --- Configuration ---
PROJECT_ID="profitscout-lx6bb"
REGION="us-central1"
RUNTIME="python312"

# --- Deployment Commands for Ingestion Functions ---

echo "Deploying INGESTION functions..."

INGESTION_SOURCE_DIR="./ingestion"

gcloud functions deploy refresh-fundamentals \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$INGESTION_SOURCE_DIR \
  --entry-point=refresh_fundamentals \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy refresh-prices \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$INGESTION_SOURCE_DIR \
  --entry-point=update_prices \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy extract-sec-filings \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$INGESTION_SOURCE_DIR \
  --entry-point=extract_sec_filings \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy refresh-financials \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$INGESTION_SOURCE_DIR \
  --entry-point=load_statements \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy populate-price-data \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$INGESTION_SOURCE_DIR \
  --entry-point=populate_price_data \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy refresh-technicals \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$INGESTION_SOURCE_DIR \
  --entry-point=refresh_technicals \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy refresh-stock-metadata \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$INGESTION_SOURCE_DIR \
  --entry-point=refresh_stock_metadata_http \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy fetch-news \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$INGESTION_SOURCE_DIR \
  --entry-point=fetch_news \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy refresh-calendar-events \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$INGESTION_SOURCE_DIR \
  --entry-point=refresh_calendar_events \
  --trigger-http \
  --allow-unauthenticated


# --- Deployment Commands for Enrichment Functions ---

echo "Deploying ENRICHMENT functions..."

ENRICHMENT_SOURCE_DIR="./enrichment"

gcloud functions deploy business-summarizer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$ENRICHMENT_SOURCE_DIR \
  --entry-point=run_business_summarizer \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy financials-analyzer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$ENRICHMENT_SOURCE_DIR \
  --entry-point=run_financials_analyzer \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy fundamentals-analyzer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$ENRICHMENT_SOURCE_DIR \
  --entry-point=run_fundamentals_analyzer \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy mda-analyzer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$ENRICHMENT_SOURCE_DIR \
  --entry-point=run_mda_analyzer \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy news-analyzer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$ENRICHMENT_SOURCE_DIR \
  --entry-point=run_news_analyzer \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy score-aggregator \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$ENRICHMENT_SOURCE_DIR \
  --entry-point=run_score_aggregator \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy technicals-analyzer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$ENRICHMENT_SOURCE_DIR \
  --entry-point=run_technicals_analyzer \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy transcript-analyzer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$ENRICHMENT_SOURCE_DIR \
  --entry-point=run_transcript_analyzer \
  --trigger-http \
  --allow-unauthenticated

echo "All ingestion and enrichment functions deployed."