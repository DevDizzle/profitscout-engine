# src/enrichment/main.py
"""
Cloud Function entry points for the GammaRips enrichment pipelines.

This module defines HTTP-triggered functions that initiate various data
enrichment tasks. Each function invokes a corresponding pipeline from the
`core.pipelines` package to perform AI-powered analysis and summarization.

The pipelines read raw data from Cloud Storage and BigQuery, apply Vertex AI
models for insights, and write the enriched data back to storage and BigQuery.
"""

import logging

import functions_framework
from flask import Request

from .core.pipelines import (
    business_summarizer,
    financials_analyzer,
    fundamentals_analyzer,
    macro_thesis,
    mda_analyzer,
    news_analyzer,
    options_analyzer,
    options_candidate_selector,
    options_feature_engineering,
    score_aggregator,
    technicals_analyzer,
    transcript_analyzer,
)

# --- Global Initialization ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# --- Cloud Function Entry Points ---


@functions_framework.http
def run_mda_analyzer(request: Request):
    """
    HTTP-triggered function to run the MD&A analyzer pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 200.
    """
    mda_analyzer.run_pipeline()
    return "MD&A analyzer pipeline finished.", 200


@functions_framework.http
def run_transcript_analyzer(request: Request):
    """
    HTTP-triggered function to run the transcript analyzer pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 200.
    """
    transcript_analyzer.run_pipeline()
    return "Transcript analyzer pipeline finished.", 200


@functions_framework.http
def run_financials_analyzer(request: Request):
    """
    HTTP-triggered function to run the financials analyzer pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 200.
    """
    financials_analyzer.run_pipeline()
    return "Financials analyzer pipeline finished.", 200


@functions_framework.http
def run_fundamentals_analyzer(request: Request):
    """
    HTTP-triggered function to run the fundamentals analyzer pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 200.
    """
    fundamentals_analyzer.run_pipeline()
    return "Fundamentals analyzer pipeline finished.", 200


@functions_framework.http
def run_technicals_analyzer(request: Request):
    """
    HTTP-triggered function to run the technicals analyzer pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 200.
    """
    technicals_analyzer.run_pipeline()
    return "Technicals analyzer pipeline finished.", 200


@functions_framework.http
def run_news_analyzer(request: Request):
    """
    HTTP-triggered function to run the news analyzer pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 200.
    """
    news_analyzer.run_pipeline()
    return "News analyzer pipeline finished.", 200


@functions_framework.http
def run_business_summarizer(request: Request):
    """
    HTTP-triggered function to run the business summarizer pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 200.
    """
    business_summarizer.run_pipeline()
    return "Business summarizer pipeline finished.", 200


@functions_framework.http
def run_score_aggregator(request: Request):
    """
    HTTP-triggered function to run the score aggregation pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 200.
    """
    score_aggregator.run_pipeline()
    return "Score aggregation pipeline finished.", 200


@functions_framework.http
def run_options_candidate_selector(request: Request):
    """HTTP Cloud Function to run the options candidate selector pipeline.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    options_candidate_selector.run_pipeline()
    return "Options candidate selector pipeline finished.", 200


@functions_framework.http
def run_options_analyzer(request: Request):
    """HTTP Cloud Function to run the options analyzer pipeline.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    options_analyzer.run_pipeline()
    return "Options analyzer pipeline finished.", 200


@functions_framework.http
def run_options_feature_engineering(request: Request):
    """HTTP Cloud Function to run the options feature engineering pipeline.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    options_feature_engineering.run_pipeline()
    return "Options feature engineering pipeline finished.", 200


@functions_framework.http
def run_thesis_generator(request: Request):
    """
    HTTP-triggered function to run the macro thesis generator pipeline.

    Args:
        request: The Flask request object (not used).

    Returns:
        A tuple containing a success message and HTTP status code 200.
    """
    macro_thesis.run_pipeline()
    return "Macro thesis generator pipeline finished.", 200
