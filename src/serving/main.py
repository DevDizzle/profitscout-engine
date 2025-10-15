# serving/main.py
import functions_framework
import logging
from flask import Request
from typing import Tuple

from .core.pipelines import (
    page_generator,
    price_chart_generator,
    data_bundler,
    sync_to_firestore,
    data_cruncher,
    dashboard_generator,
    sync_options_to_firestore,
    sync_calendar_to_firestore,
    sync_winners_to_firestore,
    recommendations_generator,
    winners_dashboard_generator,
    performance_tracker_updater,
    sync_options_candidates_to_firestore,
    sync_performance_tracker_to_firestore,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


@functions_framework.http
def run_performance_tracker_updater(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to run the performance tracker updater pipeline.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    performance_tracker_updater.run_pipeline()
    return "Performance tracker update pipeline finished.", 200


@functions_framework.http
def run_winners_dashboard_generator(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to generate the winners dashboard.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    winners_dashboard_generator.run_pipeline()
    return "Winners dashboard generator pipeline finished.", 200


@functions_framework.http
def run_recommendations_generator(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to generate recommendations.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    recommendations_generator.run_pipeline()
    return "Recommendations generator pipeline finished.", 200


@functions_framework.http
def run_sync_calendar_to_firestore(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to sync calendar events to Firestore.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    sync_calendar_to_firestore.run_pipeline()
    return "Sync calendar events to Firestore pipeline finished.", 200


@functions_framework.http
def run_sync_options_to_firestore(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to sync options data to Firestore.

    This function can perform a full reset of the data if the 'full_reset'
    parameter is set to true in the request JSON payload.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    full_reset = False
    if request and request.is_json:
        data = request.get_json(silent=True)
        if data and data.get("full_reset") is True:
            full_reset = True
    sync_options_to_firestore.run_pipeline(full_reset=full_reset)
    return f"Sync options to Firestore pipeline finished. Full reset: {full_reset}", 200


@functions_framework.http
def run_sync_winners_to_firestore(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to sync winners data to Firestore.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    sync_winners_to_firestore.run_pipeline()
    return "Sync winners to Firestore pipeline finished.", 200


@functions_framework.http
def run_dashboard_generator(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to generate the dashboard.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    dashboard_generator.run_pipeline()
    return "Dashboard generator pipeline finished.", 200


@functions_framework.http
def run_data_cruncher(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to run the data cruncher pipeline.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    data_cruncher.run_pipeline()
    return "Data cruncher pipeline finished.", 200


@functions_framework.http
def run_page_generator(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to run the page generator pipeline.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    page_generator.run_pipeline()
    return "Page generator pipeline finished.", 200


@functions_framework.http
def run_price_chart_generator(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to run the price chart generator pipeline.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    price_chart_generator.run_pipeline()
    return "Price chart generator pipeline finished.", 200


@functions_framework.http
def run_data_bundler(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to run the data bundler pipeline.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    data_bundler.run_pipeline()
    return "Data bundler pipeline finished.", 200


@functions_framework.http
def run_sync_to_firestore(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to sync data to Firestore.

    This function can perform a full reset of the data if the 'full_reset'
    parameter is set to true in the request JSON payload.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    full_reset = False
    if request and request.is_json:
        data = request.get_json(silent=True)
        if data and data.get("full_reset") is True:
            full_reset = True
    sync_to_firestore.run_pipeline(full_reset=full_reset)
    return f"Sync to Firestore pipeline finished. Full reset: {full_reset}", 200


@functions_framework.http
def run_sync_options_candidates_to_firestore(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to sync options candidates to Firestore.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    sync_options_candidates_to_firestore.run_pipeline()
    return "Sync options candidates to Firestore pipeline finished.", 200


@functions_framework.http
def run_sync_performance_tracker_to_firestore(request: Request) -> Tuple[str, int]:
    """HTTP Cloud Function to sync performance tracker data to Firestore.

    Args:
        request (flask.Request): The request object.

    Returns:
        A tuple containing a confirmation message and an HTTP status code.
    """
    sync_performance_tracker_to_firestore.run_pipeline()
    return "Sync performance tracker to Firestore pipeline finished.", 200
