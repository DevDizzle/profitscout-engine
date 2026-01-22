#!/bin/bash
set -euo pipefail

PROJECT_ID="profitscout-lx6bb"
REGION="us-central1"
RUNTIME="python312"
SERVING_SOURCE_DIR="./src/serving"

deploy_http_function() {
  local function_name=$1
  local source_dir=$2
  local entry_point=$3
  local extra_args=${4:-""}

  echo "--- Deploying ${function_name} ---"
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

# Deploying all serving functions (skipping page-generator as it was just deployed)
deploy_http_function "run-price-chart-generator" "${SERVING_SOURCE_DIR}" "run_price_chart_generator"
deploy_http_function "run-data-cruncher" "${SERVING_SOURCE_DIR}" "run_data_cruncher"
deploy_http_function "recommendations-generator" "${SERVING_SOURCE_DIR}" "run_recommendations_generator"
deploy_http_function "run-dashboard-generator" "${SERVING_SOURCE_DIR}" "run_dashboard_generator"
deploy_http_function "data-bundler" "${SERVING_SOURCE_DIR}" "run_data_bundler"
deploy_http_function "run-winners-dashboard-generator" "${SERVING_SOURCE_DIR}" "run_winners_dashboard_generator"
deploy_http_function "run-performance-tracker-updater" "${SERVING_SOURCE_DIR}" "run_performance_tracker_updater"
deploy_http_function "sync-to-firestore" "${SERVING_SOURCE_DIR}" "run_sync_to_firestore"
deploy_http_function "run-sync-options-to-firestore" "${SERVING_SOURCE_DIR}" "run_sync_options_to_firestore"
deploy_http_function "run-sync-calendar-to-firestore" "${SERVING_SOURCE_DIR}" "run_sync_calendar_to_firestore"
deploy_http_function "run-sync-winners-to-firestore" "${SERVING_SOURCE_DIR}" "run_sync_winners_to_firestore"
deploy_http_function "run-sync-options-candidates-to-firestore" "${SERVING_SOURCE_DIR}" "run_sync_options_candidates_to_firestore"
deploy_http_function "run-sync-performance-tracker-to-firestore" "${SERVING_SOURCE_DIR}" "run_sync_performance_tracker_to_firestore"
deploy_http_function "run-sync-spy-to-firestore" "${SERVING_SOURCE_DIR}" "run_sync_spy_to_firestore"
deploy_http_function "social-media-poster" "${SERVING_SOURCE_DIR}" "run_social_media_poster" "--set-secrets=X_API_KEY=X_API_KEY:latest,X_API_SECRET=X_API_SECRET:latest,X_ACCESS_TOKEN=X_ACCESS_TOKEN:latest,X_ACCESS_TOKEN_SECRET=X_ACCESS_TOKEN_SECRET:latest"
