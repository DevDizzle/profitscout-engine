#!/bin/bash
#
# Deploys ONLY technicals-analyzer.

set -euo pipefail

PROJECT_ID="profitscout-lx6bb"
REGION="us-central1"
RUNTIME="python312"
ENRICHMENT_SOURCE_DIR="./src/enrichment"

deploy_http_function() {
  local function_name=$1
  local source_dir=$2
  local entry_point=$3
  local extra_args=${4:-""}

  echo "--- Deploying ${function_name} from ${source_dir} ---"

  gcloud functions deploy "${function_name}" \
    --gen2 \
    --runtime="${RUNTIME}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --source="${source_dir}" \
    --entry-point="${entry_point}" \
    --trigger-http \
    --allow-unauthenticated \
    --timeout=3600s \
    --max-instances=1 \
    $extra_args
}

echo "Deploying technicals-analyzer..."
deploy_http_function "technicals-analyzer" "${ENRICHMENT_SOURCE_DIR}" "run_technicals_analyzer"
