#!/bin/bash
# Deploy enrichment trigger to profitscout-fida8
# Run from overnight-edge-enrichment/ directory

set -e

PROJECT="profitscout-fida8"
REGION="us-central1"
FUNCTION_NAME="enrichment_trigger"

echo "Deploying $FUNCTION_NAME to $PROJECT..."

gcloud functions deploy $FUNCTION_NAME \
  --project=$PROJECT \
  --region=$REGION \
  --runtime=python312 \
  --trigger-http \
  --allow-unauthenticated \
  --entry-point=enrichment_trigger \
  --source=. \
  --memory=1024MB \
  --timeout=540s \
  --set-env-vars="PROJECT_ID=$PROJECT,DATASET=profit_scout,GCS_BUCKET=profit-scout-data" \
  --set-secrets="POLYGON_API_KEY=POLYGON_API_KEY:latest"

echo ""
echo "✅ Deployed! Test with:"
echo "curl https://$REGION-$PROJECT.cloudfunctions.net/$FUNCTION_NAME"
echo ""
echo "Next: Add Cloud Scheduler trigger for 4:25 AM EST daily:"
echo "gcloud scheduler jobs create http overnight-enrichment \\"
echo "  --project=$PROJECT \\"
echo "  --location=$REGION \\"
echo "  --schedule='25 9 * * 1-5' \\"
echo "  --time-zone='UTC' \\"
echo "  --uri='https://$REGION-$PROJECT.cloudfunctions.net/$FUNCTION_NAME' \\"
echo "  --http-method=POST"
