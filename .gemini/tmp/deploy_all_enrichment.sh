#!/bin/bash
#
# Deploys ALL Enrichment Cloud Functions for the ProfitScout project.
# This ensures that shared changes (like gcs.py retries) are propagated to all services.

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

echo "Deploying ALL ENRICHMENT functions..."

deploy_http_function "financials-analyzer" "${ENRICHMENT_SOURCE_DIR}" "run_financials_analyzer"
deploy_http_function "fundamentals-analyzer" "${ENRICHMENT_SOURCE_DIR}" "run_fundamentals_analyzer"
deploy_http_function "technicals-analyzer" "${ENRICHMENT_SOURCE_DIR}" "run_technicals_analyzer"
deploy_http_function "mda-analyzer" "${ENRICHMENT_SOURCE_DIR}" "run_mda_analyzer"
deploy_http_function "transcript-analyzer" "${ENRICHMENT_SOURCE_DIR}" "run_transcript_analyzer"
deploy_http_function "news-analyzer" "${ENRICHMENT_SOURCE_DIR}" "run_news_analyzer"
deploy_http_function "business-summarizer" "${ENRICHMENT_SOURCE_DIR}" "run_business_summarizer"
deploy_http_function "macro-thesis-generator" "${ENRICHMENT_SOURCE_DIR}" "run_thesis_generator"
deploy_http_function "score-aggregator" "${ENRICHMENT_SOURCE_DIR}" "run_score_aggregator"
deploy_http_function "options-selector" "${ENRICHMENT_SOURCE_DIR}" "run_options_candidate_selector"
deploy_http_function "options-analyzer" "${ENRICHMENT_SOURCE_DIR}" "run_options_analyzer"
deploy_http_function "options-feature-engineering" "${ENRICHMENT_SOURCE_DIR}" "run_options_feature_engineering"

echo "--- Deployment of ALL Enrichment functions complete. ---"
