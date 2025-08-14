# serving/core/bq.py
import logging
import pandas as pd
from google.cloud import bigquery

def load_df_to_bq(df: pd.DataFrame, table_id: str, project_id: str, write_disposition: str = "WRITE_TRUNCATE"):
    """Loads a pandas DataFrame into a BigQuery table."""
    if df.empty:
        logging.warning("DataFrame is empty. Skipping BigQuery load.")
        return
    
    client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logging.info(f"Loaded {job.output_rows} rows into BigQuery table: {table_id}")
    except Exception as e:
        logging.error(f"Failed to load DataFrame to {table_id}: {e}", exc_info=True)
        raise