# serving/main.py
import functions_framework
import logging
from core.pipelines import (
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@functions_framework.http
def run_performance_tracker_updater(request):
    """Runs the daily snapshot process for the performance tracker."""
    performance_tracker_updater.run_pipeline()
    return "Performance tracker update pipeline finished.", 200

@functions_framework.http
def run_winners_dashboard_generator(request):
    """Generates the main 'winners' dashboard table."""
    winners_dashboard_generator.run_pipeline()
    return "Winners dashboard generator pipeline finished.", 200

@functions_framework.http
def run_recommendations_generator(request):
    recommendations_generator.run_pipeline()
    return "Recommendations generator pipeline finished.", 200

@functions_framework.http
def run_sync_calendar_to_firestore(request):
    """
    Syncs upcoming calendar events from BigQuery to Firestore.
    """
    sync_calendar_to_firestore.run_pipeline()
    return "Sync calendar events to Firestore pipeline finished.", 200

@functions_framework.http
def run_sync_options_to_firestore(request):
    full_reset = False
    if request and request.is_json:
        data = request.get_json(silent=True)
        if data and data.get('full_reset') is True:
            full_reset = True
    sync_options_to_firestore.run_pipeline(full_reset=full_reset)
    return f"Sync options to Firestore pipeline finished. Full reset: {full_reset}", 200

@functions_framework.http
def run_sync_winners_to_firestore(request):
    """Syncs the winners dashboard data to Firestore."""
    sync_winners_to_firestore.run_pipeline()
    return "Sync winners to Firestore pipeline finished.", 200

@functions_framework.http
def run_dashboard_generator(request):
    dashboard_generator.run_pipeline()
    return "Dashboard generator pipeline finished.", 200

@functions_framework.http
def run_data_cruncher(request):
    data_cruncher.run_pipeline()
    return "Data cruncher pipeline finished.", 200

@functions_framework.http
def run_page_generator(request):
    page_generator.run_pipeline()
    return "Page generator pipeline finished.", 200

@functions_framework.http
def run_price_chart_generator(request):
    price_chart_generator.run_pipeline()
    return "Price chart generator pipeline finished.", 200

@functions_framework.http
def run_data_bundler(request):
    data_bundler.run_pipeline()
    return "Data bundler pipeline finished.", 200

@functions_framework.http
def run_sync_to_firestore(request):
    full_reset = False
    if request and request.is_json:
        data = request.get_json(silent=True)
        if data and data.get('full_reset') is True:
            full_reset = True
    sync_to_firestore.run_pipeline(full_reset=full_reset)
    return f"Sync to Firestore pipeline finished. Full reset: {full_reset}", 200

@functions_framework.http
def run_sync_options_candidates_to_firestore(request):
    """Syncs the latest options candidates to Firestore."""
    sync_options_candidates_to_firestore.run_pipeline()
    return "Sync options candidates to Firestore pipeline finished.", 200

@functions_framework.http
def run_sync_performance_tracker_to_firestore(request):
    """Syncs the performance tracker data to Firestore."""
    sync_performance_tracker_to_firestore.run_pipeline()
    return "Sync performance tracker to Firestore pipeline finished.", 200