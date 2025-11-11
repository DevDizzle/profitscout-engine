# enrichment/core/pipelines/transcript_analyzer.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs, bq
from ..clients import vertex_ai
from .helpers import load_latest_macro_thesis
import os
import re
import json

# CORRECTED: Input is now the raw transcript from the analyzer's own config
INPUT_PREFIX = config.PREFIXES["transcript_analyzer"]["input"] 
OUTPUT_PREFIX = config.PREFIXES["transcript_analyzer"]["output"]

_EXAMPLE_OUTPUT = """{
  "score": 0.38,
  "analysis": "The earnings call for AAON tilts mildly bearish. Management conceded that Q2 results 'fall short of our expectations' and issued forward-looking guidance that 'full year 2025 growth will track in the low teens with margin near 28%'. Those commitments clash with the macro baseline described above, which assumes resilient industrial demand, while echoing the counterpoint risk of softer capital spending."
}"""

def read_transcript_content(raw_json: str) -> str | None:
    """Extracts the 'content' from the raw transcript JSON."""
    try:
        data = json.loads(raw_json)
        if isinstance(data, list) and data:
            data = data[0]
        return data.get("content")
    except (json.JSONDecodeError, TypeError, IndexError):
        return None

def process_transcript(ticker: str, date_str: str):
    """
    Processes a single, raw transcript file based on the
    ticker and date from the BigQuery work list.
    """
    input_blob_name = f"{INPUT_PREFIX}{ticker}_{date_str}.json"
    output_blob_name = f"{OUTPUT_PREFIX}{ticker}_{date_str}.json"
    
    logging.info(f"[{ticker}] Generating direct transcript analysis for {date_str}")
    
    raw_json_content = gcs.read_blob(config.GCS_BUCKET_NAME, input_blob_name)
    if not raw_json_content:
        logging.error(f"[{ticker}] Could not read raw transcript content from {input_blob_name}")
        return None
        
    transcript_content = read_transcript_content(raw_json_content)
    if not transcript_content:
        logging.error(f"[{ticker}] Could not extract 'content' from {input_blob_name}")
        return None
    
    macro_thesis = load_latest_macro_thesis()
    prompt = r"""You are a sharp financial analyst evaluating an earnings call transcript to find signals that may influence the stock over the next 1–3 months.
Use **only** the full transcript provided. Your analysis **must** be grounded in the data.

### Macro Context (read carefully before analyzing)
- Baseline macro thesis: {macro_trend}
- Counterpoint / risks: {anti_thesis}

### Key Interpretation Guidelines & Data Integration
1.  **Guidance & Outlook**: Capture any new or reiterated outlook. Provide the exact forward-looking quote(s) in single quotes.
2.  **Performance vs. Expectations**: Did the company beat or miss? Cite specific metrics if available (e.g., 'net sales declined 0.6%').
3.  **Tone & Sentiment**: Describe management's tone using direct single-quoted snippets from prepared remarks or Q&A.
4.  **Macro Alignment Check**: Assess whether the outlook aligns with, contradicts, or hedges against the macro thesis above. Call out specific overlaps or tensions.
5.  **Synthesis & Scoring**: Combine these data points into a cohesive narrative before delivering the probability score. If there are no material signals, default to 0.50.

### CRITICAL FORMATTING RULE
- When including direct quotes in the 'analysis' text, you MUST use single quotes ('), not double quotes ("). This is to ensure the final JSON is clean and renders correctly.

### Example Output (for format and tone; do not copy values)
{{example_output}}

### Step-by-Step Reasoning
1.  Scan the full transcript to identify specific data points, forward-looking statements, and sentiment quotes required by the guidelines.
2.  Evaluate each forward-looking element against the macro baseline and counterpoint.
3.  Synthesize these inputs into a net bullish/bearish score.
4.  Map the net result to probability bands:
    -   0.00-0.30 → clearly bearish
    -   0.31-0.49 → mildly bearish
    -   0.50       → neutral / balanced
    -   0.51-0.69 → moderately bullish
    -   0.70-1.00 → strongly bullish
5.  Summarize the key drivers into one dense paragraph, integrating the specific data points, forward-looking quotes, and macro comparison.

### Output — return exactly this JSON, nothing else
{
  "score": <float between 0 and 1>,
  "analysis": "<One dense paragraph (150-250 words) summarizing the key themes from the call, **integrating specific figures, forward-looking quotes, and macro alignment** to support the analysis.>"
}

Provided Transcript:
{{transcript_content}}
""".replace("{{transcript_content}}", transcript_content)
    prompt = prompt.replace("{macro_trend}", macro_thesis["macro_trend"])
    prompt = prompt.replace("{anti_thesis}", macro_thesis["anti_thesis"])
    prompt = prompt.replace("{{example_output}}", _EXAMPLE_OUTPUT)

    analysis_json = vertex_ai.generate(prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, output_blob_name, analysis_json, "application/json")
    
    gcs.cleanup_old_files(config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, output_blob_name)
    
    return output_blob_name

def run_pipeline():
    """
    Runs the transcript analysis pipeline by first querying BigQuery for the
    latest work items and then processing only those that are missing.
    """
    logging.info("--- Starting Direct Transcript Analysis Pipeline ---")
    
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

    logging.info(f"Found {len(work_items)} new transcripts to analyze.")
    
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_transcript, ticker, date_str) for ticker, date_str in work_items]
        count = sum(1 for future in as_completed(futures) if future.result())
        
    logging.info(f"--- Transcript Analysis Pipeline Finished. Processed {count} new files. ---")