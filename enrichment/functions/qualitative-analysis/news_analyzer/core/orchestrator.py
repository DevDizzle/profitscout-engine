"""
Builds the prompt and calls the GenAI client for news analysis.
"""
from . import client

_BASE_NEWS_PROMPT = """
You are a seasoned market-news analyst tasked with gauging how the **news flow alone** is likely to influence a stock’s price over the next few trading sessions (≈ 1-2 weeks). Rely **exclusively** on the JSON array supplied—do **not** draw on any outside knowledge, market data, or assumptions.

Each item contains:
- **symbol** - ticker
- **publishedDate** - UTC timestamp (“YYYY-MM-DD HH:MM:SS”)
- **title**, **text** - headline & snippet
- **site** - source domain
- **url**, **image**

### Key interpretation guidelines
1. **Sentiment & Tone**
   • Positive/bullish cues → investments, raised guidance, new products, favorable regulatory moves, analyst upgrades, upbeat adjectives (“soars”, “record”, “beats”).
   • Negative/bearish cues → lawsuits, downgrades, missed guidance, layoffs, product failures, negative adjectives (“cuts”, “plunge”, “probe”).
2. **Prevalence & Consistency** - Multiple headlines with the **same** bullish (or bearish) theme amplify conviction; mixed themes reduce it.
3. **Recency Weighting** - Headlines in the last few hours carry the greatest weight; decay importance linearly over ~24 h. Ignore items > 48 h old if fresher items exist.
4. **Source Credibility & Reach** - Major outlets (WSJ, NYT, Bloomberg, CNBC, Reuters, FT) outweigh niche blogs; official statements > analyst commentary > opinion pieces.
5. **Market-Moving Magnitude** - Large-dollar commitments, government actions, litigation, or earnings surprises outweigh routine analyst chatter.
6. **No Noteworthy News** - If the array is empty **or** every headline is neutral/minor (no material impact), output a score of **0.50** and state that the flow is effectively neutral.

### Step-by-step reasoning (internal, but summarised in output)
1. **Classify sentiment** of each headline (bullish, bearish, neutral) with brief rationale.
2. **Apply recency & credibility weights**; aggregate into a net sentiment score.
3. **Assess news momentum & asymmetry** - Are bullish items dominating, or do bearish items linger? Note any decisive catalysts.
4. **Convert aggregate insight to probability** the stock **rises** soon:
   • 0.00 - 0.30  → clearly bearish  
   • 0.31 - 0.49  → slightly bearish  
   • 0.50         → neutral / balanced (including “no noteworthy news”)  
   • 0.51 - 0.69  → moderately bullish  
   • 0.70 - 1.00  → strongly bullish

### Output — return **exactly** this JSON, nothing else
{{
  "score": <float between 0 and 1>,
  "analysis": "<ONE dense paragraph (≈200-300 words) that justifies the score, weaving together the most influential headlines, their sentiment, timing, and expected price impact.>"
}}

Provided data:
{news_data}
"""

def build_prompt(news_data: str) -> str:
    """Produces the final, formatted prompt for the AI model."""
    return _BASE_NEWS_PROMPT.format(news_data=news_data)

def summarise(news_data: str) -> str:
    """Generates a news analysis by building a prompt and calling the AI client."""
    if not news_data:
        raise ValueError("News data cannot be empty.")
    
    prompt = build_prompt(news_data)
    return client.generate(prompt)
