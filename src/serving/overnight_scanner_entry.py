"""Cloud Function entry points for the Overnight Scanner."""

import json
import logging

import functions_framework
from google.cloud import bigquery

from src.enrichment.core import config
from src.enrichment.core.pipelines import overnight_scanner

logging.basicConfig(level=logging.INFO)


@functions_framework.http
def run_overnight_scanner(request):
    """Trigger the overnight scanner pipeline."""
    try:
        overnight_scanner.run_pipeline()
        return "Overnight scanner complete", 200
    except Exception as e:
        logging.error("Overnight scanner failed: %s", e, exc_info=True)
        return f"Error: {e}", 500


@functions_framework.http
def get_overnight_signals(request):
    """HTTP endpoint to get latest overnight signals."""
    bq = bigquery.Client(project=config.PROJECT_ID)
    min_score = int(request.args.get("min_score", 6))

    query = f"""
        SELECT * FROM `{config.OVERNIGHT_SIGNALS_TABLE}`
        WHERE scan_date = (SELECT MAX(scan_date) FROM `{config.OVERNIGHT_SIGNALS_TABLE}`)
          AND overnight_score >= @min_score
        ORDER BY overnight_score DESC
        LIMIT 10
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("min_score", "INT64", min_score)
        ]
    )
    rows = bq.query(query, job_config=job_config).to_dataframe()
    return (
        rows.to_json(orient="records", date_format="iso"),
        200,
        {"Content-Type": "application/json"},
    )
