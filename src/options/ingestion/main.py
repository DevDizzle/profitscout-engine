# ingestion/main.py
import logging
import os
import functions_framework
from google.cloud import bigquery
from flask import Request
from typing import Tuple, Union

from .core import config
from .core.clients.polygon import PolygonClient
from .core.pipelines import options_chain_fetcher

# --- Global Initialization (Shared Across Functions) ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


@functions_framework.http
def fetch_options_chain(request: Request) -> Union[Tuple[str, int], str]:
    """HTTP Cloud Function to fetch options chain data.

    This function is triggered by an HTTP request. It fetches options chain data
    from the Polygon API and loads it into a BigQuery table.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a message and an HTTP status code, or just a message.
    """
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        return "POLYGON_API_KEY not set", 500

    bq_client = bigquery.Client(project=config.PROJECT_ID)

    polygon_client = PolygonClient(api_key=api_key)
    options_chain_fetcher.run_pipeline(
        polygon_client=polygon_client, bq_client=bq_client
    )
    return "Options chain fetch started.", 202
