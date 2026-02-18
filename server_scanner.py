"""
Cloud Run entry point for Overnight Scanner + Get Signals API.
Replaces the old Cloud Functions: run-overnight-scanner, get-overnight-signals.
"""

import json
import logging
import os

from flask import Flask, request, jsonify
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-fida8")
DATASET = os.getenv("BIGQUERY_DATASET", "profit_scout")
SIGNALS_TABLE = f"{PROJECT_ID}.{DATASET}.overnight_signals"


@app.route("/scan", methods=["GET", "POST"])
def run_scanner():
    """Trigger the overnight scanner pipeline."""
    try:
        from src.enrichment.core.pipelines import overnight_scanner
        overnight_scanner.run_pipeline()
        return jsonify({"status": "success", "message": "Overnight scanner complete"}), 200
    except Exception as e:
        logger.error("Overnight scanner failed: %s", e, exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/signals", methods=["GET"])
def get_signals():
    """HTTP endpoint to get latest overnight signals."""
    bq = bigquery.Client(project=PROJECT_ID)
    min_score = int(request.args.get("min_score", 6))
    limit = int(request.args.get("limit", 50))

    query = f"""
        SELECT * FROM `{SIGNALS_TABLE}`
        WHERE scan_date = (SELECT MAX(scan_date) FROM `{SIGNALS_TABLE}`)
          AND overnight_score >= @min_score
        ORDER BY overnight_score DESC
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("min_score", "INT64", min_score),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )
    rows = bq.query(query, job_config=job_config).to_dataframe()
    return (
        rows.to_json(orient="records", date_format="iso"),
        200,
        {"Content-Type": "application/json"},
    )


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "overnight-scanner"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
