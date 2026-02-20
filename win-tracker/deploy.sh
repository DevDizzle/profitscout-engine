#!/bin/bash
# Deploy win-tracker to Cloud Run
set -e

gcloud run deploy win-tracker \
  --project=profitscout-fida8 \
  --region=us-central1 \
  --source=. \
  --allow-unauthenticated \
  --memory=512Mi \
  --timeout=120 \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=1 \
  --set-secrets="POLYGON_API_KEY=POLYGON_API_KEY:latest"
