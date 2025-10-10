# enrichment/main.py

import logging
import functions_framework
from core.pipelines import (
    options_candidate_selector as candidate_selector_pipeline,
    options_analyzer as options_analyzer_pipeline,
    options_feature_engineering as options_feature_engineering_pipeline, # Add this import
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@functions_framework.http
def run_options_candidate_selector(request):
    """Programmatic options top-5 CALL/PUT selector."""
    candidate_selector_pipeline.run_pipeline()
    return "Options candidate selector pipeline finished.", 200

@functions_framework.http
def run_options_analyzer(request):
    """Triggers the options analysis pipeline."""
    options_analyzer_pipeline.run_pipeline()
    return "Options analyzer pipeline finished.", 200

@functions_framework.http
def run_options_feature_engineering(request):
    """Triggers the options feature engineering pipeline."""
    options_feature_engineering_pipeline.run_pipeline()
    return "Options feature engineering pipeline finished.", 200