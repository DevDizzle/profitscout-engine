import logging
import functions_framework
from core.pipelines import (
    mda_summarizer, mda_analyzer, transcript_summarizer, transcript_analyzer,
    financials_analyzer, metrics_analyzer, ratios_analyzer, technicals_analyzer, news_analyzer
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
def run_metrics_analyzer(request):
    metrics_analyzer.run_pipeline()
    return "Key metrics analyzer pipeline finished.", 200

@functions_framework.http
def run_ratios_analyzer(request):
    ratios_analyzer.run_pipeline()
    return "Ratios analyzer pipeline finished.", 200

@functions_framework.http
def run_technicals_analyzer(request):
    technicals_analyzer.run_pipeline()
    return "Technicals analyzer pipeline finished.", 200

@functions_framework.http
def run_news_analyzer(request):
    news_analyzer.run_pipeline()
    return "News analyzer pipeline finished.", 200
