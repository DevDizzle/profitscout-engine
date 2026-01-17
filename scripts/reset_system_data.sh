#!/bin/bash

# scripts/reset_system_data.sh
# -----------------------------------------------------------------------------
# DANGER: This script DELETES operational data to allow for a clean reset/re-seed.
# It truncates BigQuery tables and removes GCS data folders.
# -----------------------------------------------------------------------------

PROJECT_ID="profitscout-lx6bb"
DATASET_ID="profit_scout"
BUCKET_NAME="profit-scout-data"

# Tables to truncate
TABLES=(
    "stock_metadata"
    "price_data"
    "options_chain"
    "calendar_events"
    "spy_price_history"
)

# GCS folders to delete (recursively)
GCS_FOLDERS=(
    "earnings-call-transcripts/"
    "key-metrics/"
    "financial-statements/"
    "ratios/"
    "technicals/"
    "prices/"
    "sec-business/"
    "sec-mda/"
    "sec-risk/"
)

# -----------------------------------------------------------------------------

echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "WARNING: YOU ARE ABOUT TO DELETE DATA FROM PROJECT: $PROJECT_ID"
echo "THIS ACTION IS IRREVERSIBLE."
echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo ""
echo "This will TRUNCATE the following BigQuery tables:"
for t in "${TABLES[@]}"; do echo " - $DATASET_ID.$t"; done
echo ""
echo "This will DELETE all objects in the following GCS paths:"
for f in "${GCS_FOLDERS[@]}"; do echo " - gs://$BUCKET_NAME/$f"; done
echo ""

read -p "Are you ABSOLUTELY SURE you want to proceed? (Type 'YES' to confirm): " confirm

if [ "$confirm" != "YES" ]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "--- Starting System Reset ---"

# 1. BigQuery Truncation
echo ">>> Truncating BigQuery tables..."
for table in "${TABLES[@]}"; do
    echo "    Truncating $DATASET_ID.$table..."
    # Check if table exists first to avoid error spam
    if bq show "$PROJECT_ID:$DATASET_ID.$table" >/dev/null 2>&1; then
        bq query --use_legacy_sql=false "TRUNCATE TABLE $PROJECT_ID.$DATASET_ID.$table"
    else
        echo "    (Table $table does not exist, skipping)"
    fi
done

# 2. GCS Cleanup
echo ">>> Cleaning GCS buckets..."
for folder in "${GCS_FOLDERS[@]}"; do
    path="gs://$BUCKET_NAME/$folder"
    echo "    Removing objects in $path..."
    # Check if any objects exist before trying to delete
    if gcloud storage ls "$path" >/dev/null 2>&1; then
        gcloud storage rm -r "$path"
    else
        echo "    (Path $path is empty or does not exist, skipping)"
    fi
done

echo ""
echo "--- Reset Complete ---"
echo ""
echo "Next Steps:"
echo "1. Upload your new ticker list to: gs://$BUCKET_NAME/tickerlist.txt"
echo "   (Example: gsutil cp tickerlist.txt gs://$BUCKET_NAME/tickerlist.txt)"
echo "2. Run the ingestion pipelines in the recommended order (metadata -> prices -> etc.)"
