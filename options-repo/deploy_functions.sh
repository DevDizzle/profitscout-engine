#!/bin/bash

# deploy_functions.sh
# This script deploys all Cloud Functions for the profitscout-options repository.

# --- Configuration ---
PROJECT_ID="profitscout-lx6bb"
REGION="us-central1"
RUNTIME="python312"

# --- Ingestion Functions ---
echo "--- Deploying INGESTION functions ---"
INGESTION_SOURCE_DIR="./ingestion"

gcloud functions deploy options-fetcher \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$INGESTION_SOURCE_DIR \
  --entry-point=fetch_options_chain \
  --trigger-http \
  --allow-unauthenticated

# --- Enrichment Functions ---
echo "--- Deploying ENRICHMENT functions ---"
ENRICHMENT_SOURCE_DIR="./enrichment"

gcloud functions deploy options-feature-engineering \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$ENRICHMENT_SOURCE_DIR \
  --entry-point=run_options_feature_engineering \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy options-selector \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$ENRICHMENT_SOURCE_DIR \
  --entry-point=run_options_candidate_selector \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy options-analyzer \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$ENRICHMENT_SOURCE_DIR \
  --entry-point=run_options_analyzer \
  --trigger-http \
  --allow-unauthenticated

# --- Serving Functions ---
echo "--- Deploying SERVING functions ---"
SERVING_SOURCE_DIR="./serving"

gcloud functions deploy run-data-cruncher \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SERVING_SOURCE_DIR \
  --entry-point=run_data_cruncher \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy recommendations-generator \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SERVING_SOURCE_DIR \
  --entry-point=run_recommendations_generator \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy page-generator \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SERVING_SOURCE_DIR \
  --entry-point=run_page_generator \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy run-dashboard-generator \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SERVING_SOURCE_DIR \
  --entry-point=run_dashboard_generator \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy run-winners-dashboard-generator \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SERVING_SOURCE_DIR \
  --entry-point=run_winners_dashboard_generator \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy run-performance-tracker-updater \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SERVING_SOURCE_DIR \
  --entry-point=run_performance_tracker_updater \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy data-bundler \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SERVING_SOURCE_DIR \
  --entry-point=run_data_bundler \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy sync-to-firestore \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SERVING_SOURCE_DIR \
  --entry-point=run_sync_to_firestore \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy run-sync-options-to-firestore \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SERVING_SOURCE_DIR \
  --entry-point=run_sync_options_to_firestore \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy run-sync-calendar-to-firestore \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SERVING_SOURCE_DIR \
  --entry-point=run_sync_calendar_to_firestore \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy run-sync-winners-to-firestore \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SERVING_SOURCE_DIR \
  --entry-point=run_sync_winners_to_firestore \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy run-sync-options-candidates-to-firestore \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SERVING_SOURCE_DIR \
  --entry-point=run_sync_options_candidates_to_firestore \
  --trigger-http \
  --allow-unauthenticated

gcloud functions deploy price-chart-generator \
  --gen2 \
  --runtime=$RUNTIME \
  --project=$PROJECT_ID \
  --region=$REGION \
  --source=$SERVING_SOURCE_DIR \
  --entry-point=run_price_chart_generator \
  --trigger-http \
  --allow-unauthenticated

echo "All functions for profitscout-options have been deployed."