import logging
import pandas as pd
from google.cloud import bigquery
from . import config

def _get_bq_client() -> bigquery.Client:
    """Initializes and returns a BigQuery client."""
    return bigquery.Client(project=config.PROJECT_ID)

def load_df_to_bq(df: pd.DataFrame, table_id: str):
    """
    Loads a pandas DataFrame into a BigQuery table, overwriting the existing data.
    This is used to load the raw scores for the current run.
    """
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Overwrite the table with new scores each run
    )
    job = _get_bq_client().load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    logging.info(f"Loaded {job.output_rows} rows into BigQuery table: {table_id}")

def run_synthesis_query() -> pd.DataFrame:
    """
    Executes a multi-step SQL query in BigQuery to normalize, weight,
    and aggregate scores, returning the final results as a DataFrame.
    """
    client = _get_bq_client()

    # Dynamically build the normalization part of the query
    min_max_calcs = ",\n".join(
        f"      MIN({col}) AS min_{col.replace('_score','')}, MAX({col}) AS max_{col.replace('_score','')}"
        for col in config.SCORE_WEIGHTS.keys()
    )
    
    normalization_calcs = ",\n".join(
        f"      SAFE_DIVIDE(s.{col} - m.min_{col.replace('_score','')}, m.max_{col.replace('_score','')} - m.min_{col.replace('_score','')}) AS norm_{col.replace('_score','')}"
        for col in config.SCORE_WEIGHTS.keys()
    )

    # Dynamically build the final weighted sum
    weighted_sum_calcs = " +\n".join(
        f"    norm_{col.replace('_score','')} * {weight}"
        for col, weight in config.SCORE_WEIGHTS.items()
    )

    # Use the corrected table_id from the user's feedback
    table_id = "profitscout-lx6bb.profit_scout.analysis_scores"

    query = f"""
    WITH
      -- Step 1: Find the min and max of each score for the entire dataset
      MinMax AS (
        SELECT
{min_max_calcs}
        FROM
          `{table_id}`
      ),
      -- Step 2: Normalize each score to a 0-1 scale to ensure fair weighting
      NormalizedScores AS (
        SELECT
          s.ticker,
          s.run_date,
{normalization_calcs}
        FROM
          `{table_id}` AS s,
          MinMax AS m
      )
    -- Step 3: Apply the configured weights and calculate the final aggregate score
    SELECT
      ticker,
      run_date,
      (
{weighted_sum_calcs}
      ) AS weighted_score
    FROM
      NormalizedScores;
    """
    
    logging.info("Executing normalization, weighting, and aggregation query in BigQuery...")
    
    try:
        # Execute the query and return the results as a pandas DataFrame
        results_df = client.query(query).to_dataframe()
        logging.info(f"Successfully calculated weighted scores for {len(results_df)} tickers.")
        return results_df
    except Exception as e:
        logging.error(f"BigQuery synthesis query failed: {e}", exc_info=True)
        # Re-raise the exception to halt the pipeline if SQL fails
        raise