#!/bin/bash
set -e

PROJECT="profitscout-fida8"
REGION="us-central1"

echo "========================================="
echo "STEP 1: DELETE ALL CLOUD FUNCTIONS"
echo "========================================="
for fn in get-overnight-signals run-overnight-scanner track-signal-performance enrichment-trigger enrichment_trigger run_momentum_scanner; do
  echo "Deleting function: $fn"
  gcloud functions delete "$fn" --project=$PROJECT --region=$REGION --gen2 --quiet 2>/dev/null || echo "  (not found or already deleted)"
done

echo ""
echo "========================================="
echo "STEP 2: DEPLOY enrichment-trigger"
echo "========================================="
cd ~/gammarips-engine/overnight-edge-enrichment/

gcloud run deploy enrichment-trigger \
  --project=$PROJECT \
  --region=$REGION \
  --source=. \
  --allow-unauthenticated \
  --memory=1Gi \
  --timeout=540 \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=3 \
  --set-env-vars="PROJECT_ID=$PROJECT,DATASET=profit_scout,GCS_BUCKET=profit-scout-data,MODEL_NAME=gemini-3-flash-preview,VERTEX_PROJECT=$PROJECT,VERTEX_LOCATION=global" \
  --set-secrets="POLYGON_API_KEY=polygon-api-key:latest"

echo ""
echo "========================================="
echo "STEP 3: DEPLOY win-tracker"
echo "========================================="
cd ~/gammarips-engine/overnight-edge-enrichment/win_tracker/

gcloud run deploy win-tracker \
  --project=$PROJECT \
  --region=$REGION \
  --source=. \
  --allow-unauthenticated \
  --memory=512Mi \
  --timeout=300 \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=2 \
  --set-secrets="POLYGON_API_KEY=polygon-api-key:latest,X_API_KEY=x-api-key:latest,X_API_SECRET=x-api-secret:latest,X_ACCESS_TOKEN=x-access-token:latest,X_ACCESS_SECRET=x-access-secret:latest"

echo ""
echo "========================================="
echo "STEP 4: VERIFY"
echo "========================================="
echo "Cloud Run services:"
gcloud run services list --project=$PROJECT --region=$REGION

echo ""
echo "Cloud Functions (should be empty):"
gcloud functions list --project=$PROJECT --region=$REGION 2>/dev/null || echo "  None"

echo ""
echo "========================================="
echo "DONE. Update Cloud Scheduler jobs to point at new URLs."
echo "========================================="
