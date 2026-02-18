#!/bin/bash
# Deploy Agent Arena to Cloud Run
set -e

PROJECT="profitscout-fida8"
REGION="us-central1"
SERVICE="agent-arena"

echo "🏟️  Deploying Agent Arena to Cloud Run..."

gcloud run deploy $SERVICE \
  --project=$PROJECT \
  --region=$REGION \
  --source=. \
  --platform=managed \
  --allow-unauthenticated \
  --memory=1Gi \
  --cpu=1 \
  --timeout=900 \
  --concurrency=1 \
  --min-instances=0 \
  --max-instances=1 \
  --set-secrets="XAI_API_KEY=XAI_API_KEY:latest,ANTHROPIC_API_KEY=ANTHROPIC_API_KEY:latest,ARENA_GOOGLE_API_KEY=ARENA_GOOGLE_API_KEY:latest,HF_TOKEN=HF_TOKEN:latest,OPENAI_API_KEY=OPENAI_API_KEY:latest" \
  --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT"

echo "✅ Agent Arena deployed!"

# Get URL
URL=$(gcloud run services describe $SERVICE --project=$PROJECT --region=$REGION --format='value(status.url)')
echo "🔗 URL: $URL"
echo ""
echo "Test: curl -X POST $URL -H 'Content-Type: application/json' -d '{\"scan_date\": \"2026-02-13\"}'"
