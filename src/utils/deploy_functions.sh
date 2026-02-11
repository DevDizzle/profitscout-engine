#!/bin/bash
#
# Deploys all Google Cloud Functions for the ProfitScout project.
#
# This script automates the deployment of ingestion, enrichment, and serving
# functions. It reads configuration variables, defines a helper function
# for deployment, and then invokes the gcloud CLI to deploy each function.
#
# Usage:
#   Run this script from the root of the repository:
#   ./src/utils/deploy_functions.sh
#
# Prerequisites:
#   - Google Cloud SDK (`gcloud`) installed and authenticated.
#   - The target Google Cloud project is configured (`gcloud config set project [PROJECT_ID]`).

# --- Configuration ---
# Exit immediately if a command exits with a non-zero status.
set -euo pipefail

# Project and deployment configuration.
PROJECT_ID="profitscout-lx6bb"
REGION="us-central1"
RUNTIME="python312"

# Source directories for the functions.
INGESTION_SOURCE_DIR="./src/ingestion"
ENRICHMENT_SOURCE_DIR="./src/enrichment"
SERVING_SOURCE_DIR="./src/serving"

# --- Helper Function for Deployment ---

# Deploys a Gen2 HTTP-triggered Cloud Function.
#
# Args:
#   $1: The name of the Cloud Function to deploy.
#   $2: The source code directory for the function.
#   $3: The entry point (function name in the source code).
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

# --- Ingestion Functions ---
echo "Deploying INGESTION functions..."
deploy_http_function "refresh-prices" "${INGESTION_SOURCE_DIR}" "update_prices"
deploy_http_function "fetch-news" "${INGESTION_SOURCE_DIR}" "fetch_news"
deploy_http_function "refresh-fundamentals" "${INGESTION_SOURCE_DIR}" "refresh_fundamentals"
deploy_http_function "refresh-technicals" "${INGESTION_SOURCE_DIR}" "refresh_technicals"
deploy_http_function "extract-sec-filings" "${INGESTION_SOURCE_DIR}" "extract_sec_filings"
deploy_http_function "refresh-financials" "${INGESTION_SOURCE_DIR}" "load_statements"
deploy_http_function "populate-price-data" "${INGESTION_SOURCE_DIR}" "run_price_populator"
deploy_http_function "refresh-stock-metadata" "${INGESTION_SOURCE_DIR}" "refresh_stock_metadata_http"
deploy_http_function "refresh-calendar-events" "${INGESTION_SOURCE_DIR}" "refresh_calendar_events"
deploy_http_function "sync-spy-price-history" "${INGESTION_SOURCE_DIR}" "sync_spy_price_history"
deploy_http_function "options-fetcher" "${INGESTION_SOURCE_DIR}" "fetch_options_chain"
deploy_http_function "refresh-transcripts" "${INGESTION_SOURCE_DIR}" "refresh_transcripts"


# --- Enrichment Functions ---
echo "Deploying ENRICHMENT functions..."
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

# --- Serving Functions ---
echo "Deploying SERVING functions..."
deploy_http_function "run-price-chart-generator" "${SERVING_SOURCE_DIR}" "run_price_chart_generator"
deploy_http_function "run-data-cruncher" "${SERVING_SOURCE_DIR}" "run_data_cruncher"
deploy_http_function "recommendations-generator" "${SERVING_SOURCE_DIR}" "run_recommendations_generator"
deploy_http_function "page-generator" "${SERVING_SOURCE_DIR}" "run_page_generator"
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

echo "--- All functions deployed successfully. ---"