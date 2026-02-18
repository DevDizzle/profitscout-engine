#!/bin/bash
set -e

# Prepare src directory
rm -rf src
mkdir -p src/enrichment/core/pipelines
mkdir -p src/enrichment/core/clients

cp ../../src/enrichment/core/config.py src/enrichment/core/
cp ../../src/enrichment/core/pipelines/overnight_scanner.py src/enrichment/core/pipelines/
cp ../../src/enrichment/core/clients/polygon_client.py src/enrichment/core/clients/

# Create __init__.py files
touch src/__init__.py
touch src/enrichment/__init__.py
touch src/enrichment/core/__init__.py
touch src/enrichment/core/pipelines/__init__.py
touch src/enrichment/core/clients/__init__.py

# Deploy
gcloud run deploy overnight-scanner \
  --project=profitscout-fida8 \
  --region=us-central1 \
  --source=. \
  --allow-unauthenticated \
  --memory=2Gi \
  --timeout=540 \
  --cpu=2 \
  --min-instances=0 \
  --max-instances=2 \
  --set-env-vars="PROJECT_ID=profitscout-fida8,DATASET=profit_scout" \
  --set-secrets="POLYGON_API_KEY=POLYGON_API_KEY:latest"

# Cleanup
rm -rf src