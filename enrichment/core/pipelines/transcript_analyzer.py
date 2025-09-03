import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs, bq
from ..clients import vertex_ai
import os
import re

INPUT_PREFIX = config.PREFIXES["transcript_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["transcript_analyzer"]["output"]

# --- MODIFIED: A more concise one-shot example ---
_EXAMPLE_OUTPUT = """{
  "score": 0.65,
  "analysis": "The earnings call suggests a moderately bullish outlook. Management reported a 15% YoY revenue increase, driven by strong cloud division bookings, and raised its full-year EPS guidance on better-than-expected margin expansion. The overall tone was optimistic, citing 'accelerating momentum' and a strong product pipeline to counter competitive pressures. Despite some caution on European headwinds, a confident Q&A session and consistent buybacks signal a positive near-term outlook."
}"""

def process_summary(ticker: str, date_str: str):
    """
    Processes a single, targeted transcript summary file based on the
    ticker and date from the BigQuery work list.
    """
    input_blob_name = f"{INPUT_PREFIX}{ticker}_{date_str}.txt"
    output_blob_name = f"{OUTPUT_PREFIX}{ticker}_{date_str}.json"
    
    logging.info(f"[{ticker}] Generating transcript analysis for {date_str}")
    
    summary_content = gcs.read_blob(config.GCS_BUCKET_NAME, input_blob_name)
    if not summary_content:
        logging.error(f"[{ticker}] Could not read summary content from {input_blob_name}")
        return None
    
    # --- MODIFIED: Updated prompt for a shorter, more direct analysis ---
    prompt = r"""You are a sharp financial analyst evaluating an earnings call summary to find signals that may influence the stock over the next 1–3 months.
Use **only** the summary provided.

### Key Interpretation Guidelines
1.  **Guidance & Outlook**: Was guidance raised, lowered, or maintained?
2.  **Performance vs. Expectations**: Did the company beat or miss on key metrics?
3.  **Tone & Sentiment**: Was management's tone confident or cautious?
4.  **No Material Signals**: If balanced or neutral, output 0.50.

### Example Output (for format only; do not copy wording)
{{example_output}}

### Step-by-Step Reasoning
1.  Identify forward-looking statements (guidance).
2.  Assess the overall tone and key performance metrics.
3.  Synthesize these points into a net bullish/bearish score.
4.  Map the net result to probability bands:
    -   0.00-0.30 → clearly bearish
    -   0.31-0.49 → mildly bearish
    -   0.50       → neutral / balanced
    -   0.51-0.69 → moderately bullish
    -   0.70-1.00 → strongly bullish
5.  Summarize the key drivers into one dense paragraph.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (150-200 words) summarizing the key themes from the call, management's tone, and the likely impact on the stock.>"
}

Provided data:
{{summary_content}}
""".replace("{{summary_content}}", summary_content).replace("{{example_output}}", _EXAMPLE_OUTPUT)

    analysis_json = vertex_ai.generate(prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, output_blob_name, analysis_json, "application/json")
    
    gcs.cleanup_old_files(config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, output_blob_name)
    
    return output_blob_name

def run_pipeline():
    """
    Runs the transcript analysis pipeline by first querying BigQuery for the
    latest work items and then processing only those that are missing.
    """
    logging.info("--- Starting Transcript Analysis Pipeline ---")
    
    work_list_df = bq.get_latest_transcript_work_list()
    if work_list_df.empty:
        logging.info("No work items returned from BigQuery. Exiting.")
        return

    work_items = []
    for _, row in work_list_df.iterrows():
        ticker = row['ticker']
        date_str = row['date_str']
        expected_output = f"{OUTPUT_PREFIX}{ticker}_{date_str}.json"
        
        if not gcs.blob_exists(config.GCS_BUCKET_NAME, expected_output):
            work_items.append((ticker, date_str))
            
    if not work_items:
        logging.info("All latest transcript analyses are already up-to-date.")
        return

    logging.info(f"Found {len(work_items)} new transcript summaries to analyze.")
    
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_summary, ticker, date_str) for ticker, date_str in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
        
    logging.info(f"--- Transcript Analysis Pipeline Finished. Processed {count} new files. ---")