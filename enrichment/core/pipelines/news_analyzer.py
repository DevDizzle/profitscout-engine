import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re

INPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["output"]

# --- MODIFIED: A more concise one-shot example ---
_EXAMPLE_OUTPUT = """{
  "score": 0.55,
  "analysis": "Recent news for Acadia Healthcare presents a mixed but slightly bullish picture. A positive Q2 earnings beat, driven by higher admissions, is tempered by a subsequent stock selloff noted by Seeking Alpha. Key concerns cited include ongoing DOJ/SEC investigations and Medicaid headwinds, which create uncertainty. While the earnings report provides a near-term positive catalyst, these lingering legal and operational risks prevent a more decisive bullish outlook, suggesting potential for moderate but capped upward movement."
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
    
    analysis_blob_path = f"{OUTPUT_PREFIX}{ticker}_news.json"
    logging.info(f"[{ticker}] Generating news analysis for {date_str}")
    
    content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    if not content:
        return None
    
    # --- MODIFIED: Updated prompt for a shorter, more direct analysis ---
    prompt = r"""You are a sharp market-news analyst evaluating how a few key headlines might influence a stock’s price over the next 1–2 weeks.
Use **only** the JSON array supplied.

### Key Interpretation Guidelines
1.  **Sentiment & Tone**: Are the headlines positive or negative?
2.  **Consistency**: Do the articles present a unified or a mixed picture?
3.  **Magnitude**: Are these major news items (earnings, lawsuits) or minor updates?
4.  **No Noteworthy News**: If the array is empty or neutral, output 0.50.

### Example Output (for format only; do not copy values or wording)
{{example_output}}

### Step-by-Step Reasoning
1.  Classify each headline as bullish, bearish, or neutral.
2.  Aggregate into a net sentiment score.
3.  Map the net result to probability bands:
    -   0.00-0.30 → clearly bearish
    -   0.31-0.49 → mildly bearish
    -   0.50       → neutral / balanced
    -   0.51-0.69 → moderately bullish
    -   0.70-1.00 → strongly bullish
4.  Summarize the decisive themes into one dense paragraph.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (150-250 words) weaving together the most influential headlines and their expected price impact.>"
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