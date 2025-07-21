"""
Builds the strict five-section prompt and calls the GenAI client.
"""
from datetime import datetime
from . import client

# ── FIVE-SECTION PROMPT (unchanged) ────────────────────────────
_BASE_PROMPT = """
You are a financial analyst summarizing an earnings call transcript for **{ticker}** (Q{quarter} {year}). Use chain-of-thought reasoning to extract key information and words/phrases signaling short-term stock movements (30–90 days), producing a dense, narrative summary for ML embedding and NLP tasks (LDA, FinBERT). Generalize for Russell 1000 sectors. First, identify surprises (beats/misses), guidance changes, tone shifts vs. prior quarters, and Q&A dynamics (analyst questions, management responses). Second, extract 5-10 topic keywords per section (e.g., 'growth, risks, margins'). Third, condense into coherent paragraphs with metrics/quotes integrated naturally, maximizing density (no bullets, full sentences).

**Key Financial Metrics**
[Reason step-by-step: Scan for revenue/EPS/margins/FCF/segments, YoY %/vs. consensus, surprises. Condense into 1-2 dense paragraphs with exact figures and signal words (e.g., 'beat,' 'exceeded'). End with 'Topic keywords: keyword1, keyword2, ...'.]

**Key Discussion Points**
[Reason step-by-step: Pull 3-5 events/strategies/operations/Q&A insights with quotes (e.g., 'strong quarter'). Condense into narrative paragraph. End with 'Topic keywords: keyword1, keyword2, ...'.]

**Sentiment Tone**
[Reason step-by-step: Assess overall tone (Positive/Neutral/Negative) from remarks/Q&A/signals, compare to prior quarter, list drivers. Condense into paragraph. End with 'Topic keywords: keyword1, keyword2, ...'.]

**Short-Term Outlook**
[Reason step-by-step: Infer impact from guidance/macro/positioning/signals. Condense into paragraph with projections. End with 'Topic keywords: keyword1, keyword2, ...'.]

**Forward-Looking Signals**
[Reason step-by-step: Extract 4-6 insights (guidance/liquidity/Q&A/macro) with values/quotes. Condense into paragraph. End with 'Topic keywords: keyword1, keyword2, ...'.]

**Q&A Summary**
[Reason step-by-step: Identify Q&A section (e.g., after 'Operator' or 'Q&A' marker, speaker changes). Extract 2-4 key analyst questions/responses, focusing on management tone (confident/defensive) and analyst reactions (e.g., 'great results'). Include 1-2 direct quotes. Condense into dense paragraph with signal words. End with 'Topic keywords: keyword1, keyword2, ...'.]

**CRITICAL INSTRUCTIONS**:
- Use only the transcript JSON for **{ticker}** Q{quarter} {year} (e.g., content arrays with speaker labels).
- Do not invent facts; ground in evidence.
- Output six sections as dense paragraphs (250-350 words total); no lists/bullets.
- Integrate signal words/quotes naturally (e.g., 'Management noted "robust demand"').
- If data missing, note briefly (e.g., 'No Q&A provided, but guidance strong').
- Optimize for embeddings/LDA/FinBERT: Dense, signal-rich text.
- For Q&A, prioritize unscripted management responses and analyst sentiment.

---
**Transcript**:
{transcript}
"""

def build_prompt(transcript: str, ticker: str, year: int, quarter: int) -> str:
    """Produces the final, formatted prompt for the AI model."""
    return _BASE_PROMPT.format(
        ticker=ticker,
        quarter=quarter,
        year=year,
        transcript=transcript,
    )

def summarise(transcript: str, ticker: str, year: int, quarter: int) -> str:
    """Generates a summary by building a prompt and calling the AI client."""
    if not all([transcript, ticker, year, quarter]):
        raise ValueError("Transcript, ticker, year, and quarter are all required.")
    
    prompt = build_prompt(transcript, ticker=ticker, year=year, quarter=quarter)
        
    return client.generate(prompt)
