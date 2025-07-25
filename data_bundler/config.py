#!/usr/bin/env python3
"""
Configuration for the Data Bundler service.
"""
from typing import Dict

# GCS bucket where the data pipeline artifacts are stored.
# This should be the same bucket used by all other services.
BUCKET_NAME = "profit-scout-data"

# GCS path for the list of tickers to process.
TICKER_LIST_PATH = "tickerlist.txt"

# GCS folder where the final combined JSON bundles will be uploaded.
BUNDLE_OUTPUT_FOLDER = "bundles"

# Descriptions for each data section, used in the final bundle.
DOC_DESCRIPTIONS: Dict[str, str] = {
    "earnings-call": (
        "Summary of quarterly earnings calls with sentiment analysis, key metrics (EPS, "
        "revenue, net income), and management guidance. Includes related data such as "
        "analyst estimates and stock recommendations."
    ),
    "financial-statements": (
        "Quarterly and annual financial statements including income statement (revenue, "
        "expenses, net income, EPS), balance sheet (assets, liabilities, equity), and cash "
        "flow statement (operating, investing, financing cash flows)."
    ),
    "fundamentals": (
        "Key financial metrics and ratios for valuation and peer comparison, such as P/E "
        "ratio, debt to equity, ROE, ROA, current ratio, EPS, dividend yield, market cap, "
        "beta, and analyst estimates."
    ),
    "sec-business": (
        "Business overview from SEC filings (Item 1), detailing company operations, products, "
        "services, market segments, and competitive landscape."
    ),
    "sec-mda": (
        "Management’s Discussion & Analysis (MD&A) from SEC filings (Item 7/2), covering "
        "financial condition, results of operations, liquidity, capital resources, and critical "
        "accounting policies."
    ),
    "sec-risk": (
        "Risk Factors section from SEC filings, outlining potential risks and uncertainties "
        "including business, financial, operational, market, and regulatory risks."
    ),
    "technicals": (
        "Daily technical indicators including moving averages (SMA, EMA, WMA, DEMA, TEMA), "
        "RSI, MACD, Bollinger Bands, standard deviation, Williams %R, ADX, and other tools "
        "for technical analysis."
    ),
    "prices": "The last 90 days of daily stock prices.",
    "ml_predictions": "ML model predictions for 30-day stock movement post-earnings, with confidence scores.",
}

# GCS path templates for each data section.
# The '{t}' placeholder will be replaced with the ticker symbol.
SECTION_GCS_PATHS: Dict[str, str] = {
    "earnings-call": "earnings-call/json",
    "financial-statements": "financial-statements/{t}/",
    "fundamentals": "fundamentals/{t}/",
    "prices": "prices/{t}/90_day_prices.json",
    "sec-business": "sec-business/",
    "sec-mda": "sec-mda/",
    "sec-risk": "sec-risk/",
    "technicals": "technicals/{t}/technicals.json",
    "ml_predictions": "ml_predictions/{t}/predictions.json",
}