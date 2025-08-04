"""
Builds the prompt and calls the GenAI client for financial analysis.
"""
from . import client

# FIX: Escaped the curly braces in the JSON example with double braces {{ }}
# This prevents the .format() method from misinterpreting them.
_BASE_PROMPT = """
You are a seasoned financial analyst and accountant tasked with evaluating a company's financial health based solely on the provided quarterly financial data in JSON format. Do not use any external knowledge, market data, or assumptions—base your analysis entirely on the trends observable in this data.

The data includes quarterly reports, each with sections like income_statement, balance_sheet, and cash_flow_statement. Key metrics may include (but are not limited to): revenue, costOfRevenue, grossProfit, grossProfitRatio, operatingIncome, netIncome, eps, ebitda, cashAndCashEquivalents, totalCurrentAssets, totalAssets, totalCurrentLiabilities, totalLiabilities, totalDebt, netDebt, operatingCashFlow, freeCashFlow, capitalExpenditure, and others as present.

Step-by-step reasoning:
1. Identify and extract key financial metrics across quarters, focusing on the most recent quarters for trends (e.g., Q2 2025 back to earlier periods).
2. Analyze trends for each major metric: Is it trending up or down over time? Is the trend favorable (e.g., increasing revenue or cash flow, decreasing debt) or unfavorable (e.g., rising costs eroding margins, growing liabilities)?
3. Assess overall financial health: Consider profitability (e.g., margins, net income), liquidity (e.g., cash, current ratios), solvency (e.g., debt levels, equity), efficiency (e.g., cash flow generation), and growth (e.g., revenue trajectory). Highlight interconnections, like how operating cash flow supports debt reduction or how margins impact earnings.
4. Reason like an accountant: Be precise, objective, and insightful—note anomalies, improvements, risks, or sustainability of trends. For example, if revenue is up but margins are down due to higher costs, discuss implications for future profitability.

Based on these trends alone, estimate the likelihood that the stock price will increase in the near term (next 6-12 months). Express this as a score between 0 and 1:
- 0 = Definitely going down (e.g., deteriorating fundamentals across the board).
- 1 = Definitely going up (e.g., strong positive trends in growth, profitability, and stability).
Use a value like 0.7 for moderately positive trends. Anchor the score strictly to the financial data's implications for valuation and investor sentiment.

Output in this exact structured JSON format, with no additional text:
{{
  "score": <float between 0 and 1>,
  "analysis": "<A single, dense paragraph (200-400 words) summarizing key insights, trends (up/down, favorable/unfavorable), interesting findings, and accountant-style reasoning on the company's financial health and trajectory.>"
}}

Provided data:
{financial_data}
"""

def build_prompt(financial_data: str) -> str:
    """Produces the final, formatted prompt for the AI model."""
    return _BASE_PROMPT.format(financial_data=financial_data)

def summarise(financial_data: str) -> str:
    """Generates a financial analysis by building a prompt and calling the AI client."""
    if not financial_data:
        raise ValueError("Financial data cannot be empty.")
    
    prompt = build_prompt(financial_data)
        
    return client.generate(prompt)