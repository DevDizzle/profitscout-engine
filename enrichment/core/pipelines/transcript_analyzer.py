import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs, bq
from ..clients import vertex_ai
import os
import re

# Define the input and output prefixes from the central configuration
INPUT_PREFIX = config.PREFIXES["transcript_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["transcript_analyzer"]["output"]

# Example output for consistent formatting
_EXAMPLE_OUTPUT = """{
  "score": 0.65,
  "analysis": "The earnings call transcript for the quarter suggests a moderately bullish outlook. Management highlighted robust revenue growth of 15% year-over-year, driven primarily by strong performance in the cloud services division, which saw a 25% increase in bookings. This was partially offset by a slight decline in the legacy hardware segment. A key positive was the upward revision of full-year EPS guidance, which management attributed to better-than-expected margin expansion. During the Q&A, analysts focused on the competitive landscape, and management responded confidently, citing a recent key customer win and a strong product pipeline. The overall tone was optimistic, with multiple references to 'accelerating momentum' and 'sustained demand.' While some caution was expressed regarding macroeconomic headwinds in Europe, the company's strong balance sheet and consistent share buyback program provide a solid foundation. The combination of a top-line beat, raised guidance, and confident management tone points to likely positive stock performance in the near term."
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
    
    # Construct the prompt for the language model
    prompt = r"""You are a seasoned financial analyst evaluating an **earnings call summary** to judge how the narrative may influence the stock over the next 1–3 months.
Use **only** the summary provided — do **not** use external data or assumptions.

### Key Interpretation Guidelines
1.  **Guidance & Outlook** — Raised/lifted guidance is strongly bullish; cautious or lowered guidance is bearish.
2.  **Performance vs. Expectations** — Explicit beats on revenue/EPS are bullish; misses are bearish.
3.  **Tone & Sentiment** — Confident, optimistic language ("strong demand," "accelerating") is bullish; defensive language ("headwinds," "challenging environment") is bearish.
4.  **Key Business Drivers** — Growth in core products/segments is bullish; weakness is bearish.
5.  **Q&A Session Insights** — Confident, direct answers are bullish; evasiveness is bearish.
6.  **No Material Signals** — If balanced or neutral, output 0.50.

### Example Output (for format only; do not copy wording)
EXAMPLE_OUTPUT:
{{example_output}}

### Step-by-Step Reasoning
1.  Identify all forward-looking statements (guidance) and classify their direction.
2.  Note any explicit performance metrics (revenue growth, margin trends).
3.  Assess the overall tone of management's commentary and Q&A responses.
4.  Synthesize these points into a net bullish/bearish score.
5.  Map the net result to probability bands:
    -   0.00-0.30 → clearly bearish
    -   0.31-0.49 → mildly bearish
    -   0.50       → neutral / balanced
    -   0.51-0.69 → moderately bullish
    -   0.70-1.00 → strongly bullish
6.  Summarize the key drivers into one dense paragraph.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (~200-300 words) summarizing the key themes from the call, management's tone, and the likely impact on the stock.>"
}

Provided data:
{{summary_content}}
""".replace("{{summary_content}}", summary_content).replace("{{example_output}}", _EXAMPLE_OUTPUT)

    # Generate the analysis and write it to GCS
    analysis_json = vertex_ai.generate(prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, output_blob_name, analysis_json, "application/json")
    
    # Clean up older versions of the analysis for this ticker
    gcs.cleanup_old_files(config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, output_blob_name)
    
    return output_blob_name

def run_pipeline():
    """
    Runs the transcript analysis pipeline by first querying BigQuery for the
    latest work items and then processing only those that are missing.
    """
    logging.info("--- Starting Transcript Analysis Pipeline ---")
    
    # 1. Get the definitive list of the single latest summary for each ticker from BigQuery
    work_list_df = bq.get_latest_transcript_work_list()
    if work_list_df.empty:
        logging.info("No work items returned from BigQuery. Exiting.")
        return

    # 2. Determine which analyses are missing
    work_items = []
    for _, row in work_list_df.iterrows():
        ticker = row['ticker']
        date_str = row['date_str']
        expected_output = f"{OUTPUT_PREFIX}{ticker}_{date_str}.json"
        
        # Check if the final analysis file already exists in GCS
        if not gcs.blob_exists(config.GCS_BUCKET_NAME, expected_output):
            work_items.append((ticker, date_str))
            
    if not work_items:
        logging.info("All latest transcript analyses are already up-to-date.")
        return

    logging.info(f"Found {len(work_items)} new transcript summaries to analyze.")
    
    # 3. Process the missing items in parallel
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        # We submit a tuple of (ticker, date_str) to the worker function
        futures = [executor.submit(process_summary, ticker, date_str) for ticker, date_str in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
        
    logging.info(f"--- Transcript Analysis Pipeline Finished. Processed {count} new files. ---")