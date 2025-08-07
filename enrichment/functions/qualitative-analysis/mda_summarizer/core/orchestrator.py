"""
Builds the prompt and calls the GenAI client for MD&A summarization.
"""
from . import client

# ── MD&A SUMMARIZER PROMPT ─────────────────────────────────────
_BASE_PROMPT = """
You are a financial analyst summarizing the MD&A (Management’s Discussion & Analysis) for {ticker} (Q{quarter} {year}) from a {filing_type} filing.
Use step-by-step reasoning to extract the drivers of performance, liquidity, risks, and forward signals that matter for short-term stock movement (30–90 days).
Produce ONE dense, coherent narrative optimized for FinBERT embeddings and NLP tasks (e.g., LDA). Generalize to all Russell 1000 sectors.

Pre-processing rules:
• Decode HTML entities (e.g., &#8217; → ’, &nbsp; → space).
• Use only the provided MD&A text; do not invent facts or pull outside info.
• If a data point is missing, state this briefly (e.g., “No guidance provided.”).

Reasoning Steps (use privately; do not output as bullets):
1) Results of Operations & Drivers
   - Quantify YoY/QoQ changes in revenue, margins (gross/operating), EPS (if present), and segment/geography contributions.
   - Attribute movements to concrete drivers (pricing, mix, volumes/traffic, utilization, FX, cost inflation/deflation, supply chain, one-offs).
   - Note non-recurring items versus run-rate impacts.

2) Non-GAAP & Adjustments
   - Extract non-GAAP metrics named (e.g., adjusted EPS, EBITDA, FCF, CASM ex-fuel, PRASM, ARR/NRR, same-store sales).
   - Summarize reconciliation highlights and whether adjustments appear recurring (restructuring, litigation, SBC, acquisition-related) or one-time.

3) Liquidity & Capital Resources
   - Report liquidity (cash, short-term investments, undrawn facilities), cash flows (CFO/CFI/CFF) and major drivers.
   - Capital allocation and balance sheet: capex, buybacks/dividends, debt issuance/repayment, leverage trends, maturity “walls”, covenant considerations.
   - Note any rating, collateral, LTV or minimum-liquidity covenants referenced.

4) Costs & Efficiency
   - Identify cost line trends (COGS, opex, wage/benefit inflation, selling expense, fuel/energy/commodities) and productivity/efficiency programs.
   - Call out unit economics/KPIs when present (e.g., CASM/PRASM/TRASM for airlines; opex per unit, utilization, yields, backlog turns, GM% for hardware; NIM/CET1 for banks; ARR/GRR/NRR/Gross Margin for SaaS; same-store sales for retail; realized prices/production for energy).

5) Risk Factors & Exogenous Events
   - Extract macro/industry risks (rates, FX, tariffs, regulatory, litigation, safety incidents, recalls), supply chain dependencies, hedging posture, and sensitivity commentary (e.g., ±$X per 1% move in rates/fuel).

6) Outlook & Guidance
   - Capture explicit guidance (ranges, raised/lowered/affirmed), qualitative outlook, demand commentary, and management tone (confident/cautious/mixed).
   - Note leading indicators (bookings, backlog, pipeline, traffic, PRASM, order intake, churn, net adds).

7) Signal Synthesis for Near-Term Price Impact (30–90 days)
   - Weave the above into a concise assessment of likely direction and volatility catalysts (e.g., lowered guide + margin compression = negative skew; cost actions + debt paydown = supportive).
   - Prefer concrete, MD&A-grounded phrases (e.g., “expects lower domestic demand,” “committed to $X capex,” “covenant headroom of…”).

Final Output Instructions:
• Produce one continuous, dense narrative (~800–1,000 words) in full sentences and paragraphs. No section headers or bullet lists in the body.
• Integrate specific figures/percentages where given; avoid tables.
• End with exactly one line:
  Topic keywords: keyword1, keyword2, …
  (Provide 20–30 comma-separated keywords/phrases capturing metrics, drivers, risks, guidance, and catalysts. No quotes.)

Input MD&A text:
{mda}
"""

def build_prompt(mda_content: str, ticker: str, year: int, quarter: int, filing_type: str) -> str:
    """Produces the final, formatted prompt for the AI model."""
    return _BASE_PROMPT.format(
        mda=mda_content,
        ticker=ticker,
        year=year,
        quarter=quarter,
        filing_type=filing_type
    )

def summarise(mda_content: str, ticker: str, year: int, quarter: int, filing_type: str) -> str:
    """Generates a summary by building a prompt and calling the AI client."""
    if not all([mda_content, ticker, year, quarter, filing_type]):
        raise ValueError("MD&A content, ticker, year, quarter, and filing type are required.")

    prompt = build_prompt(mda_content, ticker, year, quarter, filing_type)
    return client.generate(prompt)