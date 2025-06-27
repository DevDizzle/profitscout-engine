# transcript_summarizer/core/orchestrator.py
"""
Builds the strict five-section prompt and calls the GenAI client.
"""
from datetime import datetime
from . import client

# ── FIVE-SECTION PROMPT (unchanged) ────────────────────────────
_BASE_PROMPT = """
You are a financial analyst summarizing an earnings call transcript for **{ticker}** (Q{quarter} {year}). Your goal is to extract key information and specific words/phrases that signal potential short-term stock price movements (30–90 days), producing a concise summary suitable for embedding as features in predictive models. Focus on precise metrics, strategic initiatives, Q&A sentiment, operational details, liquidity, and sentiment-rich language, generalizable across all Russell 1000 companies (e.g., technology, retail, energy, financials, healthcare).

---

**Instructions**:

**Key Financial Metrics**
- Summarize key financial results for the reported quarter, including revenue, EPS, operating income/margin, free cash flow, and segment/vertical performance (e.g., ‘retail grew 4%’) or category performance (e.g., ‘battery sales strong’) if available.
- Include year-over-year (YoY) growth, comparison to consensus expectations, and exact figures (e.g., $2.37B, +1.3% YoY).
- Highlight signal words (e.g., ‘beat,’ ‘exceeded,’ ‘declined’) in context.
- If no metrics provided, state: ‘No financial metrics provided.’
- Format: ‘[Metric]: [Value, % YoY, vs. consensus if available, with signal words].’
- Example: ‘Revenue: $2.37B, +1.3% YoY, exceeded consensus $2.35B.’

**Key Discussion Points**
- Extract 3–5 major events, decisions, or surprises (e.g., product launches, restructuring, client wins) with potential stock price impact.
- Include one strategic initiative with specifics (e.g., scale, timeline, impact) and signal words (e.g., ‘accelerated,’ ‘transformational’).
- Include one operational highlight (e.g., ‘100% nuclear uptime,’ ‘client retention improved’) with signal words, if available.
- Include one Q&A insight (e.g., analyst reaction, management confidence) with a direct quote (e.g., ‘nice results’), if available.
- Format: ‘[Speaker] – [Event, with specifics and signal words].’
- Example: ‘Analyst – Noted “good progress,” affirming strategic execution.’

**Sentiment Tone**
- State the overall tone (Positive, Neutral, Negative), based on management remarks, Q&A, and signal words (e.g., ‘confident,’ ‘challenges’).
- Compare to prior quarters (e.g., shift from cautious to optimistic), grounding in transcript evidence or inferred context (e.g., ‘post-prior losses,’ ‘post-integration challenges’).
- List 2–3 drivers (e.g., EPS beat, Q&A positivity, margin pressure).
- Example: ‘Positive, shifted from cautious 2023 due to prior losses; driven by EPS beat, confident Q&A remarks.’

**Short-Term Outlook**
- Predict stock price impact (30–90 days) using financials, guidance, strategic moves, Q&A sentiment, liquidity, and macro factors.
- Highlight guidance (e.g., raised, unchanged), macro influences (e.g., tariffs, consumer spending), competitive positioning, and signal words (e.g., ‘robust,’ ‘headwinds’).
- Example: ‘Positive outlook driven by raised EPS guidance and robust client wins, despite FX headwinds.’

**Forward-Looking Signals**
- Extract 4–6 forward-looking insights, including:
  - At least one guidance-related signal (e.g., revenue, EPS, margin) with projected value, comparison to consensus/prior guidance, and signal words (e.g., ‘raised,’ ‘conservative’).
  - One Q&A insight (e.g., analyst positivity, management confidence) with a direct quote (e.g., ‘analysts praised “spectacular year”’), if available.
  - One liquidity or capital allocation signal (e.g., ‘$1.9B cash,’ ‘$1.45B asset sale proceeds’).
  - Additional signals on macro factors, competitive positioning, operational metrics, or one-time events (e.g., ‘asset sale completed’).
- If no guidance, state: ‘No guidance provided’ and include other signals.
- Format: ‘[Topic]: [Description, with specifics and signal words].’
- Example: ‘Liquidity: $1.9B cash from asset sale, bolstering financial strength.’

---

**CRITICAL INSTRUCTIONS**:
- Use only the provided transcript for **{ticker}** Q{quarter} {year}.
- Do not invent facts, use speculation, or reference external data.
- Output only the structured analysis (the five sections above, each as a header followed by bullet points).
- Highlight signal-rich words/phrases naturally.
- Include at least one Q&A insight with a direct quote, one operational detail, one liquidity/capital signal, and prior-quarter sentiment evidence.
- If a section lacks data, state: ‘No [section name] data provided.’
- Keep it concise (200–300 words), precise ($1.9B cash), and generalizable across sectors.
- Optimize for downstream embedding by maximizing signal-rich content and minimizing formatting noise.

---

**Transcript for {ticker}**:
"""

def _quarter_year(date_iso: str) -> tuple[int, int]:
    dt = datetime.fromisoformat(date_iso)
    return ((dt.month - 1) // 3 + 1, dt.year)

def build_prompt(transcript: str, *, ticker: str, date: str) -> str:
    q, y = _quarter_year(date)
    return _PROMPT.format(ticker=ticker, quarter=q, year=y, transcript=transcript)

def summarise(transcript: str, *, ticker: str, date: str) -> str:
    return client.generate(build_prompt(transcript, ticker=ticker, date=date))