import os
import logging
from flask import Flask, jsonify, request
from google.cloud import bigquery
from src.enrichment.core import config
import pandas as pd

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route("/", methods=["GET"])
def get_signals():
    """Get latest overnight signals."""
    try:
        bq = bigquery.Client(project=config.PROJECT_ID)
        min_score_str = request.args.get("min_score", "6")
        try:
            min_score = int(min_score_str)
        except ValueError:
            min_score = 6

        query = f"""
            SELECT * FROM `{config.OVERNIGHT_SIGNALS_TABLE}`
            WHERE scan_date = (SELECT MAX(scan_date) FROM `{config.OVERNIGHT_SIGNALS_TABLE}`)
              AND overnight_score >= @min_score
            ORDER BY overnight_score DESC
            LIMIT 50
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("min_score", "INT64", min_score)
            ]
        )
        # Using pandas to fetch and serialize is easiest for JSON output
        rows = bq.query(query, job_config=job_config).to_dataframe()
        
        # Convert to JSON string directly (handles dates)
        return rows.to_json(orient="records", date_format="iso"), 200, {"Content-Type": "application/json"}

    except Exception as e:
        logger.error("Get signals failed: %s", e, exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
