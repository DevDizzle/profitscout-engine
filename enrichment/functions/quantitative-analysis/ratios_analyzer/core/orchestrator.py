"""
Builds the prompt and calls the GenAI client for key ratios analysis.
"""
from . import client

_BASE_PROMPT = """
You are a seasoned fundamental analyst evaluating a company **solely** from the *key-ratios* JSON provided.
The array always contains **eight consecutive quarters** (index 0 = most-recent, index 7 = oldest).
**No external data, news, or assumptions**—reason only from these numbers.

### Ratio Groups (may appear with different casing but same meaning)

• **Liquidity** – currentRatio, quickRatio, cashRatio
• **Working-Capital Efficiency** – daysOfSalesOutstanding, daysOfInventoryOutstanding, daysOfPayablesOutstanding, operatingCycle, cashConversionCycle
• **Profitability & Margins** – grossProfitMargin, operatingProfitMargin, pretaxProfitMargin, netProfitMargin, returnOnAssets, returnOnEquity, returnOnCapitalEmployed
• **Leverage & Solvency** – debtRatio, debtEquityRatio, longTermDebtToCapitalization, totalDebtToCapitalization, interestCoverage, companyEquityMultiplier, cashFlowToDebtRatio, shortTermCoverageRatios
• **Cash-Flow Strength** – operatingCashFlowPerShare, freeCashFlowPerShare, operatingCashFlowSalesRatio, freeCashFlowOperatingCashFlowRatio, capitalExpenditureCoverageRatio, dividendPaidAndCapexCoverageRatio
• **Turnover / Asset Utilization** – receivablesTurnover, inventoryTurnover, payablesTurnover, fixedAssetTurnover, assetTurnover
• **Valuation Multiples** – priceBookValueRatio, priceEarningsRatio, priceToSalesRatio, priceToFreeCashFlowsRatio, priceToOperatingCashFlowsRatio, priceEarningsToGrowthRatio, enterpriseValueMultiple, priceFairValue, priceCashFlowRatio
• **Shareholder Policy** – dividendYield, payoutRatio, dividendPayoutRatio

---

### Step-by-Step Reasoning (internal, but summarize conclusions)

1. **Extract & Compute** – Use `code_execution` when helpful to derive:
   • Quarter-over-quarter (Q/Q) changes across eight quarters.
   • Year-over-year (latest Q vs. Q-4) comparisons.
   • Running averages/medians to contextualize the latest reading.

2. **Trend Judgement** – For each ratio group decide if it is **improving, worsening, or stable** on both Q/Q and Y/Y bases.

3. **Cross-Metric Synthesis** – Connect dots: e.g., “Net profit margin expanded 250 bps while leverage fell, yet valuation multiple compressed → attractive risk-reward.” Highlight contradictions or reinforcing signals.

4. **Risk-Reward Verdict** – Balance upside (margin expansion + lower leverage + cheap multiples) against downside (weak liquidity, falling coverage). Be precise; avoid fluff.

---

### Scoring Guidelines

Estimate probability the **stock price will rise over the next 6-12 months** **based only on these ratios**:

* **0** = Very likely to fall (deteriorating margins, rising leverage, expensive).
* **1** = Very likely to rise (consistent improvements, strong coverage, cheap).
Use intermediate values (≈0.7 moderately bullish, ≈0.4 moderately bearish).

---

### Output (exact format, no extra text)

{{
  "score": <float between 0 and 1>,
  "analysis": "<One dense 200–400-word paragraph summarizing the most material improvements/deteriorations, liquidity & leverage context, valuation insight, and clear risk-reward verdict>"
}}

Provided data:
{key_ratios_data}
"""

def build_prompt(key_ratios_data: str) -> str:
    """Produces the final, formatted prompt for the AI model."""
    return _BASE_PROMPT.format(key_ratios_data=key_ratios_data)

def summarise(key_ratios_data: str) -> str:
    """Generates a key ratios analysis by building a prompt and calling the AI client."""
    if not key_ratios_data:
        raise ValueError("Key ratios data cannot be empty.")
    
    prompt = build_prompt(key_ratios_data)
        
    return client.generate(prompt)