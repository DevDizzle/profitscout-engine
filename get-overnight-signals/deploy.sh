#!/bin/bash
set -e

# Prepare src directory
rm -rf src
mkdir -p src/enrichment/core

cp ../../src/enrichment/core/config.py src/enrichment/core/

# Create __init__.py files
touch src/__init__.py
touch src/enrichment/__init__.py
touch src/enrichment/core/__init__.py

# Deploy
gcloud run deploy get-overnight-signals \
  --project=profitscout-fida8 \
  --region=us-central1 \
  --source=. \
  --allow-unauthenticated \
  --memory=512Mi \
  --timeout=60 \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=5 \
  --set-env-vars="PROJECT_ID=profitscout-fida8,DATASET=profit_scout"

# Cleanup
rm -rf src