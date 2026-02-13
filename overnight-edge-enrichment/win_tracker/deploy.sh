#!/bin/bash
gcloud functions deploy track_signal_performance \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=. \
  --entry-point=track_signal_performance \
  --trigger-http \
  --allow-unauthenticated \
  --memory=256MB \
  --timeout=120s \
  --set-secrets="POLYGON_API_KEY=POLYGON_API_KEY:latest,X_API_KEY=X_API_KEY:latest,X_API_SECRET=X_API_SECRET:latest,X_ACCESS_TOKEN=X_ACCESS_TOKEN:latest,X_ACCESS_SECRET=X_ACCESS_SECRET:latest" \
  --project=profitscout-fida8