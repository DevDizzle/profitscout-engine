import logging
import functions_framework
from core.pipelines import (
    mda_summarizer, 
    mda_analyzer, 
    transcript_summarizer, 
    transcript_analyzer,
    financials_analyzer, 
    technicals_analyzer, 
    news_analyzer, 
    news_fetcher, 
    business_summarizer,
    fundamentals_analyzer,
    options_candidate_selector as candidate_selector_pipeline,  # <-- added
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@functions_framework.http
def run_mda_summarizer(request):
    mda_summarizer.run_pipeline()
    return "MD&A summarizer pipeline finished.", 200

@functions_framework.http
def run_mda_analyzer(request):
    mda_analyzer.run_pipeline()
    return "MD&A analyzer pipeline finished.", 200

@functions_framework.http
def run_transcript_summarizer(request):
    transcript_summarizer.run_pipeline()
    return "Transcript summarizer pipeline finished.", 200

@functions_framework.http
def run_transcript_analyzer(request):
    transcript_analyzer.run_pipeline()
    return "Transcript analyzer pipeline finished.", 200

@functions_framework.http
def run_financials_analyzer(request):
    financials_analyzer.run_pipeline()
    return "Financials analyzer pipeline finished.", 200

@functions_framework.http
def run_fundamentals_analyzer(request):
    """Entry point for the combined fundamentals analysis pipeline."""
    fundamentals_analyzer.run_pipeline()
    return "Fundamentals analyzer pipeline finished.", 200

@functions_framework.http
def run_technicals_analyzer(request):
    technicals_analyzer.run_pipeline()
    return "Technicals analyzer pipeline finished.", 200

@functions_framework.http
def run_news_fetcher(request):
    news_fetcher.run_pipeline()
    return "News fetcher pipeline finished.", 200

@functions_framework.http
def run_news_analyzer(request):
    news_analyzer.run_pipeline()
    return "News analyzer pipeline finished.", 200

@functions_framework.http
def run_business_summarizer(request):
    business_summarizer.run_pipeline()
    return "Business summarizer pipeline finished.", 200

# --- NEW: Options candidate selector (no LLM here) ---
@functions_framework.http
def candidate_selector(request):
    """Programmatic options top-5 CALL/PUT selector."""
    candidate_selector_pipeline.run_pipeline()
    return "Options candidate selector pipeline finished.", 200
