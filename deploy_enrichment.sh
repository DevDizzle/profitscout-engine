# deploy_enrichment.sh

# --- Configuration ---
PROJECT_ID="profitscout-lx6bb"
REGION="us-central1"
RUNTIME="python312"
SOURCE_DIR="./enrichment"

# Deploying the news_fetcher requires the POLYGON_API_KEY secret
# Ensure you have this secret created in Google Secret Manager.
POLYGON_SECRET="POLYGON_API_KEY"

# --- Deployment Commands ---

echo "Deploying business-summarizer..."
gcloud functions deploy business-summarizer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SOURCE_DIR \
  --entry-point=run_business_summarizer \
  --trigger-http \
  --allow-unauthenticated


echo "Deploying financials-analyzer..."
gcloud functions deploy financials-analyzer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SOURCE_DIR \
  --entry-point=run_financials_analyzer \
  --trigger-http \
  --allow-unauthenticated 

echo "Deploying fundamentals-analyzer..."
gcloud functions deploy fundamentals-analyzer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SOURCE_DIR \
  --entry-point=run_fundamentals_analyzer \
  --trigger-http \
  --allow-unauthenticated

echo "Deploying mda-analyzer..."
gcloud functions deploy mda-analyzer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SOURCE_DIR \
  --entry-point=run_mda_analyzer \
  --trigger-http \
  --allow-unauthenticated

echo "Deploying news-analyzer..."
gcloud functions deploy news-analyzer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SOURCE_DIR \
  --entry-point=run_news_analyzer \
  --trigger-http \
  --allow-unauthenticated 

echo "Deploying score-aggregator..."
gcloud functions deploy score-aggregator \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SOURCE_DIR \
  --entry-point=run_score_aggregator \
  --trigger-http \
  --allow-unauthenticated

echo "Deploying technicals-analyzer..."
gcloud functions deploy technicals-analyzer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SOURCE_DIR \
  --entry-point=run_technicals_analyzer \
  --trigger-http \
  --allow-unauthenticated

echo "Deploying transcript-analyzer..."
gcloud functions deploy transcript-analyzer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SOURCE_DIR \
  --entry-point=run_transcript_analyzer \
  --trigger-http \
  --allow-unauthenticated

echo "All enrichment functions deployed."