import functions_framework
import logging
from core import aggregator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@functions_framework.http
def score_aggregator(request):
    try:
        ticker_data = aggregator.gather_analysis_files()
        if not ticker_data:
            return "No valid files found.", 200

        final_df = aggregator.process_and_score_data(ticker_data)
        if final_df.empty:
            return "No data to load.", 200
            
        aggregator.load_data_to_bq(final_df)
        return "Aggregation and scoring completed.", 200

    except Exception as e:
        logging.critical(f"A critical error occurred: {e}", exc_info=True)
        return "An internal error occurred.", 500