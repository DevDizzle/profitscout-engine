# serving/functions/recommendation_generator/main.py
import functions_framework
import logging
from core import recommender

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@functions_framework.http
def generate_recommendations(request):
    """
    HTTP-triggered function to generate buy/sell/hold recommendations.
    """
    try:
        logging.info("Recommendation generator function triggered.")
        recommender.run_pipeline()
        logging.info("Recommendation generator pipeline finished successfully.")
        return "Recommendation generation pipeline finished.", 200
    except Exception as e:
        logging.critical(f"A critical error occurred in the recommendation pipeline: {e}", exc_info=True)
        return "An internal error occurred.", 500