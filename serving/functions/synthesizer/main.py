import functions_framework
import logging
from core import synthesizer

# --- Global Initialization ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@functions_framework.http
def run_synthesizer(request):
    """
    HTTP-triggered Cloud Function that orchestrates the synthesis pipeline.
    """
    logging.info("--- Starting Synthesizer Pipeline ---")

    try:
        # Step 1: Gather all scores and analysis text from GCS
        ticker_data = synthesizer.gather_analysis_files()

        if not ticker_data:
            logging.warning("No ticker data found. Exiting pipeline.")
            return "No analysis files found to process.", 200

        # Step 2: Create a DataFrame and load scores into BigQuery
        scores_df = synthesizer.create_scores_dataframe(ticker_data)
        synthesizer.load_scores_to_bq(scores_df)

        # Step 3: Run the synthesis query in BigQuery
        weighted_scores_df = synthesizer.get_weighted_scores()

        # Step 4: Generate final recommendations and save them to GCS
        count = synthesizer.generate_and_save_recommendations(weighted_scores_df, ticker_data)

        logging.info(f"--- Synthesizer Pipeline Finished Successfully. Generated {count} recommendations. ---")
        return "Synthesizer pipeline completed successfully.", 200

    except Exception as e:
        logging.critical(f"A critical error occurred in the synthesizer pipeline: {e}", exc_info=True)
        return "An internal error occurred.", 500