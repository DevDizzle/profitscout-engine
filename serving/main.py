# serving/main.py
import functions_framework
import logging
from core.pipelines import (
    score_aggregator,
    recommendation_generator,
    data_bundler,
    sync_to_firestore,
    page_generator,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@functions_framework.http
def run_score_aggregator(request):
    """Entry point for the score aggregation pipeline."""
    score_aggregator.run_pipeline()
    return "Score aggregation pipeline finished.", 200

@functions_framework.http
def run_recommendation_generator(request):
    """Entry point for the recommendation generation pipeline."""
    recommendation_generator.run_pipeline()
    return "Recommendation generation pipeline finished.", 200

@functions_framework.http
def run_data_bundler(request):
    """Entry point for the data bundler (final assembly) pipeline."""
    data_bundler.run_pipeline()
    return "Data bundler pipeline finished.", 200

@functions_framework.http
def run_sync_to_firestore(request):
    """
    Entry point for the Firestore sync pipeline.
    Accepts a JSON body with 'full_reset': true to trigger a wipe-and-reload.
    """
    full_reset = False
    try:
        request_json = request.get_json(silent=True)
        if request_json and request_json.get('full_reset') is True:
            full_reset = True
            logging.info("Full reset requested for Firestore sync.")
    except Exception as e:
        logging.warning(f"Could not parse request JSON for full_reset flag: {e}")

    sync_to_firestore.run_pipeline(full_reset=full_reset)
    msg = "Firestore sync pipeline finished with full reset." if full_reset else "Firestore sync pipeline finished."
    return msg, 200

@functions_framework.http
def run_page_generator(request):
    """Entry point for the page content generation pipeline."""
    page_generator.run_pipeline()
    return "Page generator pipeline finished.", 200