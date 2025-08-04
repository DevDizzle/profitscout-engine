"""
Builds the prompt and calls the GenAI client for key metrics analysis.
"""
from . import client

_BASE_PROMPT = """
You are a seasoned equity valuation analyst assessing a company’s fundamental attractiveness **strictly** from the *key-metrics* JSON supplied below.
**No external data, news, or assumptions**—reason only from the numbers.

The array always contains **exactly eight consecutive quarters** (index 0 = most-recent, index 7 = oldest).
Fields may include, but are not limited to:

• **Valuation Multiples** — peRatio, priceToSalesRatio, pocfRatio, pfcfRatio, pbRatio, ptbRatio, evToSales, enterpriseValueOverEBITDA, evToOperatingCashFlow, evToFreeCashFlow, earningsYield, freeCashFlowYield
• **Profitability / Returns** — netIncomePerShare, freeCashFlowPerShare, ROE, ROIC, returnOnTangibleAssets, operatingCashFlowPerShare
• **Leverage & Solvency** — debtToEquity, debtToAssets, netDebtToEBITDA, interestCoverage, interestDebtPerShare
• **Liquidity & Working Capital** — currentRatio, cashPerShare, workingCapital, netCurrentAssetValue
• **Efficiency** — daysSalesOutstanding, daysPayablesOutstanding, daysOfInventoryOnHand, receivablesTurnover, inventoryTurnover, payablesTurnover
• **Growth Signals** — revenuePerShare, cash-flow growth, margin or yield trajectories
• **Other Quality / Risk Indicators** — tangibleBookValuePerShare, Graham-style metrics, dividendYield, payoutRatio, intangiblesToTotalAssets

---

### Step-by-Step Reasoning (internal, but summarize conclusions):

1. **Extract & Compute** – Use the `code_execution` tool when helpful to calculate:
   • Quarter-over-quarter deltas (Q/Q) for each metric across the eight-quarter span.
   • Year-over-year comparisons (latest Q vs. the one four quarters back).
   • Running means/medians to contextualize the latest figure.
2. **Trend Assessment** – For every category, judge trends as **improving, worsening, or flat** over both Q/Q and Y/Y horizons.
3. **Cross-Metric Synthesis** – Connect valuation with profitability and balance-sheet health. Highlight contradictions (e.g., cheap on PE but leverage ballooning) or reinforcing positives.
4. **Risk-Reward Verdict** – Balance upside signals (e.g., rising ROIC + multiple compression) against downside risks (e.g., falling cash ratios, deteriorating interest coverage). Be precise and concise.

---

### Output Scoring

Estimate the probability that the **stock price will rise in the next 6-12 months** based **only** on these eight quarters of fundamentals:

* **0** = Very likely to fall (widespread deterioration, over-levered, expensive).
* **1** = Very likely to rise (consistent improvements, solid balance sheet, attractive valuation).
Use intermediate values (e.g., 0.7 for moderately bullish).

---

### Required Output (exact format):

{{
  "score": <float between 0 and 1>,
  "analysis": "<One dense, 200-400-word paragraph summarizing the most material improvements/deteriorations, valuation context, leverage & liquidity considerations, and a clear risk-reward verdict>"
}}

Provided data:
{key_metrics_data}
"""

def build_prompt(key_metrics_data: str) -> str:
    """Produces the final, formatted prompt for the AI model."""
    return _BASE_PROMPT.format(key_metrics_data=key_metrics_data)

def summarise(key_metrics_data: str) -> str:
    """Generates a key metrics analysis by building a prompt and calling the AI client."""
    if not key_metrics_data:
        raise ValueError("Key metrics data cannot be empty.")
    
    prompt = build_prompt(key_metrics_data)
        
    return client.generate(prompt)