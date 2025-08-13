import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re

INPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["output"]

# One-shot example for consistent output format (format anchor only)
_EXAMPLE_OUTPUT = """{
  "score": 0.55,
  "analysis": "The news flow surrounding Acadia Healthcare (ACHC) presents a mixed picture, leading to a slightly bullish outlook. The most recent headline highlights a Q2 earnings beat driven by growing admission volumes, which is a positive signal. However, a Seeking Alpha article tempers this enthusiasm, noting an earnings selloff and cautioning against buying despite undervaluation based on P/S and P/E ratios. This article cites ongoing DOJ/SEC investigations and Medicaid headwinds as persistent concerns, further highlighting reputational risks, softness in Medicaid-driven business, legal costs, CFO resignation, and policy uncertainty. The presence of the Q2 earnings call transcript offers some transparency but does not inherently sway sentiment in either direction. Given the conflicting signals – a strong earnings report versus lingering legal and operational concerns – the overall impact is moderately positive. The earnings beat provides a near-term catalyst, but the longer-term risks outlined in the Seeking Alpha article prevent a more decisive bullish outlook. The recency of both articles, published on the same day, suggests a relatively balanced tug-of-war between positive earnings momentum and underlying anxieties about the company's future. Therefore, the stock is likely to experience moderate upward price pressure in the next 1-2 weeks, but gains could be capped by investor caution related to the aforementioned risks."
}"""

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_2025-08-08.json'."""
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)

def process_blob(blob_name: str):
    """Processes one daily news file."""
    ticker, date_str = parse_filename(blob_name)
    if not ticker or not date_str:
        return None
    
    # The output filename is consistent, so it will overwrite previous days' analysis
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_news.json"
    logging.info(f"[{ticker}] Generating news analysis for {date_str}")
    
    content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    if not content:
        return None
    
    prompt = r"""You are a seasoned market-news analyst tasked with evaluating how **news flow alone** is likely to influence a stock’s price over the next 1–2 weeks.  
Use **only** the JSON array supplied — do **not** draw on any outside knowledge, market data, or assumptions.

### Key Interpretation Guidelines
1. **Sentiment & Tone** — Positive cues (investments, raised guidance, new products, favorable regulatory moves, analyst upgrades) are bullish; negative cues (lawsuits, downgrades, missed guidance, layoffs) are bearish.
2. **Prevalence & Consistency** — Multiple headlines with the same bullish/bearish theme amplify conviction; mixed themes dilute it.
3. **Recency Weighting** — Very recent headlines carry more weight; decay importance linearly over ~24h; ignore items >48h old if fresher ones exist.
4. **Source Credibility** — Major outlets (WSJ, Bloomberg, Reuters, CNBC, FT) outweigh niche blogs; official statements > analyst commentary > opinion pieces.
5. **Magnitude of Impact** — Large-dollar deals, government actions, litigation, or earnings surprises outweigh routine coverage.
6. **No Noteworthy News** — If the array is empty or only minor/neutral headlines exist, output 0.50 and state that the flow is neutral.

### Example Output (for format only; do not copy values or wording)
EXAMPLE_OUTPUT:
{{example_output}}

### Step-by-Step Reasoning (internal, but condensed in output)
1. Classify each headline as bullish, bearish, or neutral.
2. Apply recency and credibility weights; aggregate into a net sentiment score.
3. Assess momentum/asymmetry — are bullish items dominating or do bearish items linger?
4. Map the aggregate to a probability the stock will rise:
   - 0.00-0.30 → clearly bearish
   - 0.31-0.49 → mildly bearish
   - 0.50       → neutral / balanced
   - 0.5-0.69 → moderately bullish
   - 0.70-1.00 → strongly bullish
5. Summarize the decisive themes into one dense paragraph.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (~200-300 words) weaving together the most influential headlines, their sentiment, timing, and expected price impact.>"
}

Provided data:
{{news_data}}
""".replace("{{news_data}}", content).replace("{{example_output}}", _EXAMPLE_OUTPUT)

    analysis_json = vertex_ai.generate(prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, analysis_blob_path, analysis_json, "application/json")
    return analysis_blob_path

def run_pipeline():
    logging.info("--- Starting News Analysis Pipeline (Daily Run) ---")
    work_items = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
            
    if not work_items:
        logging.info("No new news files to process.")
        return

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
    logging.info(f"--- News Analysis Pipeline Finished. Processed {count} files. ---")
