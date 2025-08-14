# serving/functions/recommendation_generator/core/bq.py
import logging
from google.cloud import bigquery
from . import config

def get_analysis_scores() -> list[dict]:
    """
    Fetches the latest analysis scores, including the ticker,
    weighted score, and aggregated text from the BigQuery table.
    """
    client = bigquery.Client(project=config.PROJECT_ID)
    logging.info(f"Querying BigQuery for analysis scores: {config.SCORES_TABLE_ID}")

    # Query to get the necessary fields for recommendation generation
    query = f"""
        SELECT
            ticker,
            weighted_score,
            aggregated_text
        FROM
            `{config.SCORES_TABLE_ID}`
        WHERE
            weighted_score IS NOT NULL
            AND aggregated_text IS NOT NULL
    """

    try:
        # Execute the query and convert the results to a list of dictionaries
        df = client.query(query).to_dataframe()
        logging.info(f"Successfully fetched {len(df)} records for recommendation.")
        return df.to_dict('records')
    except Exception as e:
        logging.critical(f"Failed to query BigQuery for analysis scores: {e}", exc_info=True)
        return []