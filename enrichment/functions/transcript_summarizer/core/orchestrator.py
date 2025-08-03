"""
Builds the strict five-section prompt and calls the GenAI client.
"""
from datetime import datetime
from . import client

# ── FIVE-SECTION PROMPT (unchanged) ────────────────────────────
_BASE_PROMPT = """
You are a financial analyst summarizing an earnings call transcript for {ticker} (Q{quarter} {year}). Use chain-of-thought reasoning to extract key information, surprises, and words/phrases signaling short-term stock movements (30–90 days), producing a single dense, narrative summary optimized for FinBERT embeddings and NLP tasks (LDA). Generalize for Russell 1000 sectors. First, identify financial surprises (beats/misses), guidance updates, tone variations vs. prior quarters, and Q&A interactions (analyst probes, management replies). Second, extract 20-30 topic keywords overall (e.g., 'growth acceleration, margin pressure, demand surge'). Third, condense all elements into a coherent, unified narrative summary in full sentences and paragraphs, maximizing density and signal richness (no bullets, no section headers, integrate metrics/quotes naturally).Reasoning Steps:Financial Surprises and Metrics: Scan for core metrics like revenue, EPS, margins, FCF, and segments; note YoY changes, vs. consensus, and surprises with exact figures and signal phrases (e.g., 'surpassed expectations by 5%,' 'missed guidance due to supply issues'). Highlight implications for momentum.
Guidance and Forward Signals: Extract updates to outlook, projections, or macro influences; include specific values/quotes (e.g., 'raising full-year revenue guidance to $X billion amid robust demand'). Assess changes from prior calls and potential stock catalysts.
Tone and Sentiment Analysis: Evaluate overall sentiment (positive/neutral/negative) across prepared remarks and Q&A; compare to previous quarters, identify drivers like confident language ('excited about pipeline') or cautionary notes ('headwinds persisting'). Incorporate FinBERT-friendly phrases to amplify sentiment cues.
Key Discussions and Strategies: Pull 4-6 operational insights, events, or strategies with embedded quotes (e.g., 'launched new product line driving 15% growth'); focus on elements signaling upside or risks, using financially charged language.
Q&A Dynamics: Locate Q&A (via 'Operator,' speaker shifts); extract 3-5 key exchanges, emphasizing management tone (defensive/confident), analyst sentiment, and unscripted reveals (e.g., 'analyst praised "impressive turnaround," management affirmed "no further delays"').
Signal Synthesis for Stock Movement: Infer aggregated signals for short-term price probability (e.g., beats + raised guidance = positive momentum); weave in phrases indicating volatility or stability, optimizing for embedding capture of predictive signals.

Final Output Instructions:Synthesize all reasoning into one continuous, dense narrative summary (approximately 800-1,000 words to balance density and completeness).
Structure as a flowing story: begin with financial performance and surprises, transition to discussions and tone, incorporate outlook and signals, conclude with Q&A insights and overall implications.
Integrate signal words/quotes naturally (e.g., 'Executives expressed optimism over "accelerating adoption," bolstering raised EPS guidance').
End the summary with a single list: 'Topic keywords: keyword1, keyword2, ...'.
Use only the transcript JSON for {ticker} Q{quarter} {year} (e.g., content arrays with speaker labels).
Do not invent facts; ground in evidence.
If data missing, note briefly (e.g., 'Guidance unchanged, though tone upbeat').
Optimize for FinBERT embeddings: Dense, signal-rich text with financial jargon, sentiment cues, predictive phrases, and balanced positive/negative indicators.
Prioritize unscripted elements in Q&A for authentic signals.

Transcript:
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
