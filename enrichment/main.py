# enrichment/main.py
import logging
import functions_framework
from core.pipelines import (
    mda_analyzer, 
    transcript_analyzer,
    financials_analyzer, 
    technicals_analyzer, 
    news_analyzer, 
    business_summarizer,
    fundamentals_analyzer,
    score_aggregator,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@functions_framework.http
def run_mda_analyzer(request):
    mda_analyzer.run_pipeline()
    return "MD&A analyzer pipeline finished.", 200

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

@functions_framework.http
def run_score_aggregator(request):
    score_aggregator.run_pipeline()
    return "Score aggregation pipeline finished.", 200