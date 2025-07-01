# transcript_summarizer/core/orchestrator.py
"""
Builds the strict five-section prompt and calls the GenAI client.
"""
from datetime import datetime
from . import client

# ── FIVE-SECTION PROMPT (unchanged) ────────────────────────────
_BASE_PROMPT = """
You are a financial analyst summarising an earnings call transcript for **{ticker}**
(Q{quarter} {year}). Your goal is to extract key information and specific words/
phrases that signal potential short-term stock-price movements (30–90 days),
producing a concise summary suitable for embedding as features in predictive models.
Focus on precise metrics, strategic initiatives, Q&A sentiment, operational details,
liquidity, and sentiment-rich language.

**Key Financial Metrics**
- Summarise key results: revenue, EPS, operating margin, free cash flow.
- Include year-over-year growth and comparison to consensus expectations.
- Highlight signal words like ‘beat’, ‘exceeded’, or ‘declined’.

**Key Discussion Points**
- Extract 3–5 major events, decisions, or surprises with price impact.
- Include specifics on strategic initiatives (e.g. scale, timeline).
- Note one key operational highlight and one insightful Q&A exchange.

**Sentiment Tone**
- State overall tone (Positive│Neutral│Negative) vs prior quarters.
- List 2–3 drivers for the sentiment (e.g. EPS beat, margin pressure).

**Short-Term Outlook**
- Predict likely price impact (30–90 days) based on guidance, Q&A sentiment,
  and macro factors.

**Forward-Looking Signals**
- Extract 4–6 forward-looking insights: guidance, liquidity, competitive position.

**CRITICAL INSTRUCTIONS**
- Use only the provided transcript for **{ticker}** Q{quarter} {year}.
- Do NOT invent facts or use external data.
- Output only the five sections above, concise and precise.
---
**Transcript**:
{transcript}
"""

# ── Helpers ───────────────────────────────────────────────────
def _derive_quarter(dt: datetime) -> tuple[int, int]:
    return ((dt.month - 1) // 3 + 1, dt.year)

def build_prompt(transcript: str, *, ticker, quarter=None, year=None, date=None):
    """
    Produce the final prompt.  Raise if required metadata missing.
    Accepts either (ticker+quarter+year) or (ticker+date ISO yyyy-mm-dd).
    """
    if ticker and quarter and year:
        return _BASE_PROMPT.format(
            ticker=ticker,
            quarter=quarter,
            year=year,
            transcript=transcript,
        )

    if ticker and date:
        dt = datetime.fromisoformat(date)
        q, y = _derive_quarter(dt)
        return _BASE_PROMPT.format(
            ticker=ticker,
            quarter=q,
            year=y,
            transcript=transcript,
        )

    raise ValueError("ticker plus quarter & year (or ticker plus ISO date) is required")

def summarise(transcript: str, **meta) -> str:
    prompt = build_prompt(transcript, **meta)
    return client.generate(prompt)