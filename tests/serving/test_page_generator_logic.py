import pytest
from src.serving.core.pipelines.page_generator import _generate_seo

def test_generate_seo_strongly_bullish():
    ticker = "VRT"
    company = "Vertiv Holdings"
    signal = "Strongly Bullish"
    call_wall = 185.0
    
    seo = _generate_seo(ticker, company, signal, call_wall)
    
    # Check H1 contains exact signal
    assert signal in seo['h1']
    assert f"{ticker} Targets $185: {signal} Momentum Signal" == seo['h1']
    
    # Check Title contains bias (which is signal)
    assert signal in seo['title']
    
    # Check Meta Description contains lowercased signal
    assert signal.lower() in seo['metaDescription']

def test_generate_seo_neutral():
    ticker = "SPY"
    company = "SPDR S&P 500"
    signal = "Neutral"
    call_wall = 500.0
    
    seo = _generate_seo(ticker, company, signal, call_wall)
    
    assert signal in seo['h1']
    assert f"{ticker} Targets $500: {signal} Momentum Signal" == seo['h1']

def test_generate_seo_none_signal():
    ticker = "SPY"
    company = "SPDR S&P 500"
    signal = None
    call_wall = 500.0
    
    seo = _generate_seo(ticker, company, signal, call_wall)
    
    # Default to Neutral
    assert "Neutral" in seo['h1']
    assert f"{ticker} Targets $500: Neutral Momentum Signal" == seo['h1']

def test_generate_seo_empty_signal():
    ticker = "SPY"
    company = "SPDR S&P 500"
    signal = ""
    call_wall = 500.0
    
    seo = _generate_seo(ticker, company, signal, call_wall)
    
    # Default to Neutral
    assert "Neutral" in seo['h1']
