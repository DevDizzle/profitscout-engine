#!/bin/bash
# scripts/setup_observability.sh
#
# Sets up Cloud Monitoring Alerting Policies for the ProfitScout engine.
#
# This script creates:
# 1. An Email Notification Channel (interactive input required).
# 2. An Alert Policy for Cloud Function Failures (status != ok).
# 3. An Alert Policy for Cloud Workflow Failures (status != SUCCESS).
#
# Prerequisites:
#   - gcloud beta components installed (for monitoring policies).
#   - Permissions to create monitoring policies.

set -e

PROJECT_ID="profitscout-lx6bb"
DISPLAY_NAME_PREFIX="[GammaRips]"

echo "--- Setting up Observability for Project: $PROJECT_ID ---"

# 1. Create Notification Channel
echo ""
echo "Step 1: Notification Channel Setup"
echo "Existing channels:"
gcloud beta monitoring channels list --project="$PROJECT_ID" --format="table(name,displayName,labels.email_address)"

echo ""
read -p "Enter the email address to receive alerts (or press Enter to skip creation if one exists): " EMAIL_ADDRESS

CHANNEL_NAME=""

if [ -n "$EMAIL_ADDRESS" ]; then
    echo "Creating email notification channel for $EMAIL_ADDRESS..."
    CHANNEL_INFO=$(gcloud beta monitoring channels create \
        --project="$PROJECT_ID" \
        --display-name="GammaRips Ops ($EMAIL_ADDRESS)" \
        --type=email \
        --channel-labels=email_address="$EMAIL_ADDRESS" \
        --format="value(name)")
    CHANNEL_NAME="$CHANNEL_INFO"
    echo "Created channel: $CHANNEL_NAME"
else
    echo "Skipping channel creation. Please copy an existing channel ID from the list above (e.g., projects/.../notificationChannels/123...)"
    read -p "Enter Notification Channel ID: " CHANNEL_NAME
fi

if [ -z "$CHANNEL_NAME" ]; then
    echo "Error: No notification channel specified. Exiting."
    exit 1
fi

# 2. Cloud Function Failure Alert
echo ""
echo "Step 2: Creating Cloud Function Failure Alert Policy..."
gcloud alpha monitoring policies create \
    --project="$PROJECT_ID" \
    --display-name="$DISPLAY_NAME_PREFIX Cloud Function Failure" \
    --notification-channels="$CHANNEL_NAME" \
    --condition-display-name="Function Execution Error" \
    --condition-filter='resource.type = "cloud_function" AND metric.type = "cloudfunctions.googleapis.com/function/execution_count" AND metric.labels.status != "ok"' \
    --aggregation='{"alignmentPeriod": "60s", "perSeriesAligner": "ALIGN_RATE"}' \
    --duration="60s" \
    --trigger-count=1 \
    --combiner="OR" \
    --enabled \
    --policy-from-file=<(echo '{ \
      "displayName": "'$DISPLAY_NAME_PREFIX' Cloud Function Failure", \
      "combiner": "OR", \
      "conditions": [ \
        { \
          "displayName": "Function Execution Error", \
          "conditionThreshold": { \
            "filter": "resource.type = \"cloud_function\" AND metric.type = \"cloudfunctions.googleapis.com/function/execution_count\" AND metric.labels.status != \"ok\"", \
            "aggregations": [ \
              { \
                "alignmentPeriod": "300s", \
                "perSeriesAligner": "ALIGN_RATE", \
                "crossSeriesReducer": "REDUCE_SUM", \
                "groupByFields": ["resource.labels.function_name"] \
              } \
            ], \
            "comparison": "COMPARISON_GT", \
            "thresholdValue": 0, \
            "duration": "60s", \
            "trigger": { \
              "count": 1 \
            } \
          } \
        } \
      ] \
    }')

# 3. Cloud Workflow Failure Alert
echo ""
echo "Step 3: Creating Cloud Workflow Failure Alert Policy..."
gcloud alpha monitoring policies create \
    --project="$PROJECT_ID" \
    --display-name="$DISPLAY_NAME_PREFIX Workflow Failure" \
    --notification-channels="$CHANNEL_NAME" \
    --policy-from-file=<(echo '{ \
      "displayName": "'$DISPLAY_NAME_PREFIX' Workflow Failure", \
      "combiner": "OR", \
      "conditions": [ \
        { \
          "displayName": "Workflow Failed", \
          "conditionThreshold": { \
            "filter": "resource.type = \"workflows.googleapis.com/Workflow\" AND metric.type = \"workflows.googleapis.com/workflow/execution_count\" AND metric.labels.status != \"SUCCESS\"", \
            "aggregations": [ \
              { \
                "alignmentPeriod": "300s", \
                "perSeriesAligner": "ALIGN_RATE", \
                "crossSeriesReducer": "REDUCE_SUM", \
                "groupByFields": ["resource.labels.workflow_id"] \
              } \
            ], \
            "comparison": "COMPARISON_GT", \
            "thresholdValue": 0, \
            "duration": "60s", \
            "trigger": { \
              "count": 1 \
            } \
          } \
        } \
      ] \
    }')

echo ""
echo "--- Observability Setup Complete ---"
echo "Verify your alerts at: https://console.cloud.google.com/monitoring/alerting?project=$PROJECT_ID"
