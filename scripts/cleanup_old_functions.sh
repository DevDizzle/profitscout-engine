#!/bin/bash
set -euo pipefail

PROJECT_ID="profitscout-lx6bb"
REGION="us-central1"

# List of functions to delete (duplicates or incorrect names)
FUNCTIONS_TO_DELETE=(
    "fetch_options_chain"
    "fetch_transcripts"
    "run_calendar_events_loader"
    "run_fundamentals_loader"
    "load_financial_statements"
    "run_price_populator"
    "run_thesis_generator"
    "run_mda_analyzer"
    "run_page_generator"
    "run_financials_analyzer"
    "run_fundamentals_analyzer"
    "run_technicals_analyzer"
    "run_news_analyzer"
    "run_transcript_analyzer"
    "run_business_summarizer"
    "run_score_aggregator"
    "run_options_candidate_selector"
    "run_options_analyzer"
    "run_options_feature_engineering"
    "run_recommendations_generator"
    "run_data_bundler"
    "run_social_media_poster"
    "sync-winners-to-firestore"
    "update-prices"
    "data-cruncher"
    "run_sync_to_firestore"
    "transcript_summarizer"
    "load-statements"
    "ticker-selection"
    "price-loader"
    "bq-loader"
    "data_bundler"
    "run_technicals_collector"
)

echo "Starting cleanup of old/duplicate Cloud Functions..."

for func in "${FUNCTIONS_TO_DELETE[@]}"; do
    echo "Deleting function: $func"
    # Run in background to speed up? Gcloud might throttle. stick to serial or small batches.
    # Using --quiet to avoid interactive prompts
    gcloud functions delete "$func" \
        --project="$PROJECT_ID" \
        --region="$REGION" \
        --gen2 \
        --quiet || echo "Failed to delete $func or it doesn't exist. Continuing..."
done

echo "Cleanup complete."
