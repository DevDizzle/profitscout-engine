"""
Builds the prompt and calls the GenAI client for technical analysis.
"""
from . import client

_BASE_PROMPT = """
You are a seasoned technical analyst tasked with evaluating a stock's technical indicators based solely on the provided timeseries data in JSON format. Do not use any external knowledge, market data, or assumptions—base your analysis entirely on the trends observable in this data.

The data includes a ticker, as_of date, and a technicals_timeseries array with daily entries, each containing metrics like open, high, low, close, volume, SMA_50, SMA_200, EMA_21, ADX_14, DMP_14, DMN_14, MACD_12_26_9, MACDh_12_26_9, MACDs_12_26_9, RSI_14, STOCHk_14_3_3, STOCHd_14_3_3, ROC_20, Bollinger Bands (BBL_20_2.0, BBM_20_2.0, BBU_20_2.0, BBB_20_2.0, BBP_20_2.0), ATRr_14, OBV, 52w_high, 52w_low, percent_atr, and others as present. Focus on the most recent 90 days, with emphasis on the latest entries in the timeseries.

Key Interpretation Guidelines (use these to inform bullish/bearish assessments):
- Price Action (close, open, high, low): Upward closes over time or breaking 52w highs are bullish; nearing 52w lows or consistent declines are bearish.
- Moving Averages (SMA_50, SMA_200, EMA_21): Price above averages is bullish (support); below is bearish (resistance). Golden cross (SMA_50 > SMA_200) bullish; death cross bearish.
- Trend Strength/Direction (ADX_14, DMP_14, DMN_14): ADX >25 signals strong trend; DMP > DMN indicates bullish direction, reverse for bearish.
- Momentum (MACD_12_26_9 and variants): MACD line > signal line (positive histogram) is bullish; cross below is bearish. Positive values suggest upward momentum.
- Oscillators (RSI_14, STOCHk/d_14_3_3): RSI >70 overbought (bearish reversal risk), <30 oversold (bullish); 40-60 neutral. STOCH >80 overbought, <20 oversold; %K crossing above %D is bullish.
- Rate of Change (ROC_20): Positive and increasing is bullish momentum; negative is bearish.
- Volatility (Bollinger Bands, ATRr_14, percent_atr): Price near upper band (high BBP) in uptrend is bullish; near lower bearish. Band squeeze (low BBB) precedes breakouts; high ATR/percent_atr signals volatility, amplifying trends.
- Volume (OBV, volume): Rising OBV confirms bullish price moves (accumulation); falling OBV with rising prices suggests bearish divergence.

Step-by-step reasoning:
1. Identify and extract key technical metrics across the timeseries, calculating deltas or changes where useful (e.g., compare metrics from ~90 days ago to the most recent day to gauge overall change, price changes, volume shifts, indicator crossovers).
2. Analyze trends for each major metric: Is it trending up or down over time (short-term vs. long-term)? Is the trend bullish (e.g., price above moving averages, rising MACD, RSI above 50) or bearish (e.g., death cross in SMAs, declining OBV, overbought STOCH)?
3. Assess overall technical picture: Consider price action (e.g., momentum via ROC, support/resistance near 52w highs/lows), trend strength (e.g., ADX > 25 for strong trends), momentum oscillators (e.g., MACD crossovers, RSI for overbought/oversold), volatility (e.g., ATR, Bollinger Band squeezes/expansions), volume (e.g., OBV confirming trends), and interconnections (e.g., how volume supports price breakouts or how BBP indicates position within bands).
4. Reason like a technical analyst: Be precise, objective, and insightful—note patterns (e.g., bullish divergences, breakouts), deltas in indicators, potential reversals, and sustainability of trends. For example, if close is above SMA_50 but below SMA_200 with declining RSI, discuss implications for intermediate-term weakness.

Based on these technical trends alone, estimate the likelihood that the stock price will increase in the near term (next 1-3 months). Express this as a score between 0 and 1:
- 0 = Definitely going down (e.g., bearish trends across indicators).
- 1 = Definitely going up (e.g., strong bullish signals in momentum, volume, and trends).
Use a value like 0.7 for moderately bullish trends. Anchor the score strictly to the technical data's implications for price direction.

Output in this exact structured JSON format, with no additional text:
{{
  "score": <float between 0 and 1>,
  "analysis": "<A single, dense paragraph (200-400 words) summarizing key insights, trends (up/down, bullish/bearish), interesting findings, and technical analyst-style reasoning on the stock's momentum, volatility, and potential trajectory.>"
}}

Provided data:
{technicals_data}
"""

def build_prompt(technicals_data: str) -> str:
    """Produces the final, formatted prompt for the AI model."""
    return _BASE_PROMPT.format(technicals_data=technicals_data)

def summarise(technicals_data: str) -> str:
    """Generates a technical analysis by building a prompt and calling the AI client."""
    if not technicals_data:
        raise ValueError("Technicals data cannot be empty.")
    
    prompt = build_prompt(technicals_data)
        
    return client.generate(prompt)