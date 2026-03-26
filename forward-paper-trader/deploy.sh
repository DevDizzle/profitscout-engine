#!/bin/bash
# Deploy forward-paper-trader to Cloud Run
set -e

PROJECT_ID="profitscout-fida8"
REGION="us-central1"
SERVICE_NAME="forward-paper-trader"

echo "Deploying $SERVICE_NAME to Cloud Run in project $PROJECT_ID..."

gcloud run deploy $SERVICE_NAME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=. \
  --clear-base-image \
  --allow-unauthenticated \
  --memory=1024Mi \
  --timeout=600 \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=1 \
  --set-env-vars="GCP_PROJECT_ID=$PROJECT_ID" \
  --set-secrets="POLYGON_API_KEY=POLYGON_API_KEY:latest,FMP_API_KEY=FMP_API_KEY:latest" \
  --service-account="firebase-adminsdk-fbsvc@$PROJECT_ID.iam.gserviceaccount.com"

echo "Done!"