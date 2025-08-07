"""
Builds the prompt and calls the GenAI client for MD&A summary analysis.
"""
from . import client

_BASE_MDA_PROMPT = """
You are a fundamental-analysis specialist evaluating a company’s **Management’s Discussion & Analysis (MD&A) summary** to judge how its underlying fundamentals are likely to move the stock over the next one-to-three months. Use **only** the narrative supplied—do **not** import outside data, market prices, or assumptions.

### Key interpretation guidelines
1. **Growth & Profitability**
   • Revenue, gross profit, and gross-margin expansion are bullish; contraction is bearish.
   • Trend in operating income / net income: shrinking losses or rising profits are bullish; widening losses are bearish.
2. **Costs & Operating Leverage**
   • R&D and SG&A growth is fine if revenue grows faster; otherwise margin pressure is bearish.
   • Inflation, supply-chain, or wage headwinds that management says it “cannot fully offset” are bearish.
3. **Liquidity & Cash-Flow**
   • Strong operating cash flow and ample cash / working capital versus short-term obligations is bullish.
   • Negative free cash flow or looming need for external funding is bearish.
4. **Balance-Sheet Health**
   • Deleveraging, low net debt, or comfortable interest coverage is bullish; rising leverage or higher interest expense is bearish.
5. **Competitive & Cyclical Factors**
   • Securing “design wins,” expanding TAM, or product leadership is bullish.
   • Heavy reliance on cyclical end-markets or single geographies, or mention of tariffs/regulatory risk, is bearish.
6. **Outlook Tone**
   • Explicit guidance raises/lifts are bullish; cautious or uncertain tone is bearish.
7. **No Material Signals**
   • If the summary is empty **or** wholly balanced with no clear positives/negatives, output **0.50** and state that fundamentals appear neutral.

### Step-by-step reasoning (internal, but condensed in output)
1. Extract and classify each cited datapoint or management comment as bullish, bearish, or neutral.
2. Weigh them by materiality (revenue impact, magnitude of cash flow, size of geographic exposure, etc.).
3. Net the signals into a single **probability the stock price rises** in the near term:
   • 0.00-0.30  → clearly bearish fundamentals
   • 0.31-0.49  → mildly bearish
   • 0.50       → neutral / balanced (or “no material signals”)
   • 0.51-0.69  → moderately bullish
   • 0.70-1.00  → strongly bullish
4. Craft one dense paragraph (≈200-300 words) weaving together the pivotal positives and negatives that justify the score.

### Output — return **exactly** this JSON, nothing else
{{
  "score": <float between 0 and 1>,
  "analysis": "<ONE dense paragraph summarising key bullish vs bearish factors, balance-sheet strength, cash-flow trajectory, and management outlook to justify the score.>"
}}

Provided data:
{mda_summary}
"""

def build_prompt(mda_summary: str) -> str:
    """Produces the final, formatted prompt for the AI model."""
    return _BASE_MDA_PROMPT.format(mda_summary=mda_summary)

def summarise(mda_summary: str) -> str:
    """Generates an MD&A analysis by building a prompt and calling the AI client."""
    if not mda_summary:
        raise ValueError("MD&A summary data cannot be empty.")
    
    prompt = build_prompt(mda_summary)
    return client.generate(prompt)