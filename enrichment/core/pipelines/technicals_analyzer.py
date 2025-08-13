import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re

INPUT_PREFIX = config.PREFIXES["technicals_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["technicals_analyzer"]["output"]

# One-shot example for consistent output format (format anchor only)
_EXAMPLE_OUTPUT = """{
  "score": 0.35,
  "analysis": "Amazon's technical outlook over the past 90 days suggests a mildly bearish trend. While the price experienced a significant surge in late June, peaking around $223, it has since retraced, falling below the 21-day EMA, which now acts as resistance. The MACD line is trending downwards, and while still positive, the MACD histogram has been negative for much of July and early August, indicating weakening bullish momentum. The RSI has also declined from overbought territory in late June to below 50, signaling reduced buying pressure. The ADX, while above 25 for a significant portion of the period, indicating a defined trend, has recently decreased, suggesting a possible weakening of the current downtrend. The OBV shows a significant drop corresponding with the price decline, confirming the selling pressure. The stock price remains above both the 50-day and 200-day SMAs, which is a moderately bullish sign, but the recent price action suggests a potential test of these levels. The stochastic oscillator is also signaling bearish momentum, with both %K and %D below 50. Recent price action has seen lower highs and lower lows, further reinforcing the bearish sentiment. The stock is currently trading closer to its lower Bollinger Band, indicating potential for continued downside. Overall, the confluence of negative momentum indicators, recent price declines, and weakening trend strength suggests a mildly bearish outlook for AMZN in the short term, with a potential for further declines if key support levels are breached."
}"""

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_technicals.json'."""
    pattern = re.compile(r"([A-Z.]+)_technicals\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return match.group(1) if match else None

def process_blob(blob_name: str):
    """Processes one daily technicals file."""
    ticker = parse_filename(blob_name)
    if not ticker:
        return None
    
    # The output filename is consistent, so it will overwrite the previous day's analysis
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_technicals.json"
    logging.info(f"[{ticker}] Generating technicals analysis")
    
    content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    if not content:
        return None
    
    prompt = r"""You are a seasoned technical analyst evaluating a stock’s **technical indicators** over the past ~90 days to assess likely direction over the next 1–3 months.
Use **only** the JSON provided — do **not** use external data or assumptions.

### Key Interpretation Guidelines
1. **Price Action** — Rising closes or breakouts above prior highs are bullish; declines toward lows are bearish.
2. **Moving Averages** — Price above SMA/EMA is bullish; below is bearish. Golden cross bullish; death cross bearish.
3. **Trend Strength** — ADX >25 with DMP>DMN is bullish; DMN>DMP is bearish.
4. **Momentum** — Positive MACD crossovers and rising RSI (40–70) are bullish; negative crossovers and falling RSI are bearish.
5. **Oscillators** — RSI >70 or STOCH >80 is overbought (bearish reversal risk); RSI <30 or STOCH <20 is oversold (bullish reversal potential).
6. **Volatility & Volume** — Rising OBV with uptrend is bullish; falling OBV with rising price is bearish divergence.
7. **No Material Signals** — If mixed/neutral, output 0.50 and state technicals are neutral.

### Example Output (for format only; do not copy values or wording)
EXAMPLE_OUTPUT:
{{example_output}}

### Step-by-Step Reasoning
1. Evaluate recent changes in price, trend, momentum, volatility, and volume.
2. Classify as bullish, bearish, or neutral.
3. Map net result to probability bands:
   - 0.00-0.30 → clearly bearish
   - 0.31-0.49 → mildly bearish
   - 0.50       → neutral / balanced
   - 0.51-0.69 → moderately bullish
   - 0.70-1.00 → strongly bullish
4. Summarize key signals into one dense paragraph.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (200-400 words) summarizing key trends, bullish/bearish patterns, and technical reasoning on likely price trajectory.>"
}

Provided data:
{{technicals_data}}
""".replace("{{technicals_data}}", content).replace("{{example_output}}", _EXAMPLE_OUTPUT)

    analysis_json = vertex_ai.generate(prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, analysis_json, "application/json")
    return analysis_blob_path

def run_pipeline():
    logging.info("--- Starting Technicals Analysis Pipeline (Daily Run) ---")
    work_items = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
            
    if not work_items:
        logging.info("No new technicals files to process.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- Technicals Analysis Pipeline Finished. Processed {count} files. ---")
