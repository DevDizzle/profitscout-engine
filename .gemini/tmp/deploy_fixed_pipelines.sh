#!/bin/bash
set -e

echo "--- Deploying Technicals Analyzer (Wipe & Sequential) ---"
gcloud functions deploy technicals-analyzer \
    --gen2 \
    --runtime=python312 \
    --project=profitscout-lx6bb \
    --region=us-central1 \
    --source=./src/enrichment \
    --entry-point=run_technicals_analyzer \
    --trigger-http \
    --allow-unauthenticated \
    --timeout=3600s \
    --max-instances=1

echo "--- Deploying News Analyzer (Wipe & Sequential + Fix) ---"
gcloud functions deploy news-analyzer \
    --gen2 \
    --runtime=python312 \
    --project=profitscout-lx6bb \
    --region=us-central1 \
    --source=./src/enrichment \
    --entry-point=run_news_analyzer \
    --trigger-http \
    --allow-unauthenticated \
    --timeout=3600s \
    --max-instances=1

echo "--- Deploying Page Generator (Wipe & Sequential) ---"
# Note: Page Generator is in 'serving' module, not 'enrichment'
gcloud functions deploy page-generator \
    --gen2 \
    --runtime=python312 \
    --project=profitscout-lx6bb \
    --region=us-central1 \
    --source=./src/serving \
    --entry-point=run_page_generator \
    --trigger-http \
    --allow-unauthenticated \
    --timeout=3600s \
    --max-instances=1

echo "--- All Fixed Pipelines Deployed ---"
