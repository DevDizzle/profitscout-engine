#!/bin/bash
# Deploy overnight-report-generator to Cloud Run
set -e

gcloud run deploy overnight-report-generator \
  --project=profitscout-fida8 \
  --region=us-central1 \
  --source=. \
  --allow-unauthenticated \
  --memory=1Gi \
  --timeout=540 \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=2 \
  --set-env-vars="PROJECT_ID=profitscout-fida8,DATASET=profit_scout" \
  --set-secrets="GOOGLE_API_KEY=GOOGLE_API_KEY:latest"
