import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs, bq
from ..clients import vertex_ai
import os
import re

INPUT_PREFIX = config.PREFIXES["transcript_analyzer"]["input"]
OUTPUT_PREFIX = config.PREFIXES["transcript_analyzer"]["output"]

# --- MODIFIED: A data-driven one-shot example ---
_EXAMPLE_OUTPUT = """{
  "score": 0.38,
  "analysis": "The earnings call for AAON presents a mildly bearish outlook, dominated by significant operational challenges. Management explicitly stated that Q2 results 'fall short of our expectations' due to a problematic ERP system rollout that disrupted production. Consequently, the company is 'revising our full year 2025 outlook lower,' now anticipating low-teens sales growth and a gross margin of 28% to 29%. While the BasX data center business remains a strong point with sales up 127%, this was offset by declines in the core AAON brand. The combination of a clear earnings miss, lowered guidance, and ongoing operational headwinds signals near-term pressure on the stock."
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
    
    # --- MODIFIED: Updated prompt to require specific data points and quotes ---
    prompt = r"""You are a sharp financial analyst evaluating an earnings call summary to find signals that may influence the stock over the next 1–3 months.
Use **only** the summary provided. Your analysis **must** be grounded in the data.

### Key Interpretation Guidelines & Data Integration
1.  **Guidance & Outlook**: Was guidance changed? You **must** cite the specific guidance revision (e.g., "revising our full year 2025 outlook lower").
2.  **Performance vs. Expectations**: Did the company beat or miss? Cite specific metrics if available (e.g., "net sales declined 0.6%").
3.  **Tone & Sentiment**: What was management's tone? You **must** include a short, direct quote that captures their sentiment (e.g., "fall short of our expectations").
4.  **Synthesis**: Combine these data points into a cohesive narrative.
5.  **No Material Signals**: If balanced or neutral, output 0.50.

### Example Output (for format and tone; do not copy values)
{{example_output}}

### Step-by-Step Reasoning
1.  Identify and extract the specific data points and quotes required by the guidelines.
2.  Assess the overall tone and key performance metrics.
3.  Synthesize these points into a net bullish/bearish score.
4.  Map the net result to probability bands:
    -   0.00-0.30 → clearly bearish
    -   0.31-0.49 → mildly bearish
    -   0.50       → neutral / balanced
    -   0.51-0.69 → moderately bullish
    -   0.70-1.00 → strongly bullish
5.  Summarize the key drivers into one dense paragraph, integrating the specific data points you identified.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (150-250 words) summarizing the key themes from the call, **integrating specific figures and direct quotes** to support the analysis.>"
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