import logging
import os
from flask import Flask, jsonify
from src.enrichment.core.pipelines import overnight_scanner

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route("/", methods=["POST"])
def run_scanner():
    """Trigger the overnight scanner pipeline."""
    try:
        results = overnight_scanner.run_pipeline()
        count = len(results) if results else 0
        return jsonify({"status": "success", "signals_found": count}), 200
    except Exception as e:
        logger.error("Overnight scanner failed: %s", e, exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
