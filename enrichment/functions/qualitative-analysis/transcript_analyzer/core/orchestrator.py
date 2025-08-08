"""
Builds the prompt and calls the GenAI client for earnings call summary analysis.
"""
from . import client

_BASE_EARNINGS_CALL_PROMPT = """
You are an earnings-call analyst evaluating **only** the narrative supplied below (a condensed transcript/summary of the latest call) to estimate how the information and management tone are likely to influence the stock over the next one-to-three months. Do **not** bring in outside data, prior quarters, or market prices.

### Key interpretation guidelines
1. **Revenue & Demand Signals**
   • Clear beats, double-digit growth, or forward-guidance raises are bullish.
   • Misses, slowing growth, or demand warnings are bearish.
2. **Margins & Cost Structure**
   • Expanding gross, operating, or pre-tax margins, or evidence of cost leverage, is bullish.
   • Rising input costs, wage pressure, or guidance for higher unit costs that management cannot offset is bearish.
3. **Cash Flow, Debt & Capital Allocation**
   • Strong free cash flow, accelerated net-debt reduction, or shareholder-friendly returns (buybacks/dividends) are bullish.
   • Liquidity concerns, covenant pressure, or upcoming capital-raise needs are bearish.
4. **Operational Execution & Efficiency**
   • Productivity gains, supply-chain improvements, automation/digitalization that lift throughput are bullish.
   • Persistent bottlenecks, labor shortages, or major cap-ex overruns are bearish.
5. **Guidance & Management Tone**
   • Confident, specific outlook with clearly identified upside drivers is bullish.
   • Vague, cautious, or “high uncertainty” language is bearish.
6. **Competitive & Structural Factors**
   • Market-share gains, secular-growth tailwinds, IP or regulatory advantages are bullish.
   • Intensifying competition, regulatory headwinds, or over-reliance on cyclical end-markets is bearish.
7. **No Material Signals**
   • If the summary is empty **or** perfectly balanced with no clear positives/negatives, output **0.50** and state that fundamentals appear neutral.

### Step-by-step reasoning (internal, but condensed in output)
1. Classify each datapoint/comment as bullish, bearish, or neutral.
2. Weight by materiality (revenue magnitude, margin impact, cash-flow size, etc.).
3. Net the signals into a **probability the stock rises** soon:
   • 0.00-0.30  → clearly bearish
   • 0.31-0.49  → mildly bearish
   • 0.50       → neutral / balanced
   • 0.51-0.69  → moderately bullish
   • 0.70-1.00  → strongly bullish
4. Craft one dense paragraph (≈200-300 words) weaving together the pivotal positives and negatives that justify the score.

### Output — return **exactly** this JSON, nothing else
{{
  "score": <float between 0 and 1>,
  "analysis": "<ONE dense paragraph summarising key bullish vs bearish factors, margin trajectory, balance-sheet moves, guidance tone, and operational outlook to justify the score.>"
}}

Provided data:
{call_summary}
"""

def build_prompt(call_summary: str) -> str:
    """Produces the final, formatted prompt for the AI model."""
    return _BASE_EARNINGS_CALL_PROMPT.format(call_summary=call_summary)

def summarise(call_summary: str) -> str:
    """Generates a transcript analysis by building a prompt and calling the AI client."""
    if not call_summary:
        raise ValueError("Call summary data cannot be empty.")
    
    prompt = build_prompt(call_summary)
    return client.generate(prompt)