# enrichment/main.py

import logging
import functions_framework
from flask import Request
from typing import Tuple

from .core.pipelines import (
    options_candidate_selector as candidate_selector_pipeline,
    options_analyzer as options_analyzer_pipeline,
    options_feature_engineering as options_feature_engineering_pipeline,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


@functions_framework.http
def run_options_candidate_selector(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to run the options candidate selector pipeline.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    candidate_selector_pipeline.run_pipeline()
    return "Options candidate selector pipeline finished.", 200


@functions_framework.http
def run_options_analyzer(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to run the options analyzer pipeline.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    options_analyzer_pipeline.run_pipeline()
    return "Options analyzer pipeline finished.", 200


@functions_framework.http
def run_options_feature_engineering(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to run the options feature engineering pipeline.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    options_feature_engineering_pipeline.run_pipeline()
    return "Options feature engineering pipeline finished.", 200
