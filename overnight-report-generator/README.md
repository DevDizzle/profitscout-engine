# Overnight Report Generator

A production-grade Cloud Run service for GammaRips that generates the 'Overnight Edge' daily report.

## Objective
This service:
1. Reads the latest enriched overnight signal data from BigQuery (`profit_scout.overnight_signals_enriched`).
2. Generates the full Overnight Edge report using Vertex AI / Gemini 2.5 Flash in GammaRips editorial style.
3. Validates the generated content.
4. Writes the report into Firestore under the `daily_reports` collection.

## Deployment
Deployed to Google Cloud Run in the `profitscout-fida8` project.

To deploy manually:
```bash
./deploy.sh
```

## Usage
The service exposes an HTTP POST endpoint at `/`.

Payload:
```json
{
  "report_date": "2026-03-10",
  "underlying_scan_date": "2026-03-09",
  "force": true
}
```

If no payload is provided, it defaults to today as the `report_date` and yesterday as the `underlying_scan_date`.
The `force` flag bypasses the idempotency check and overwrites any existing report for that date.

## Cloud Scheduler
To run automatically, configure a Cloud Scheduler job targeting the Cloud Run endpoint:
- **Frequency**: Every weekday morning (e.g., `0 8 * * 1-5` for 8:00 AM M-F)
- **Target**: HTTP POST
- **URL**: `<Cloud Run URL>`
- **Auth**: OIDC token for the appropriate service account.
