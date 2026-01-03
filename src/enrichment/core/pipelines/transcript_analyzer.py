# enrichment/core/pipelines/transcript_analyzer.py
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from .. import config, gcs
from ..clients import vertex_ai
import os
import re
import json

INPUT_PREFIX = config.PREFIXES["transcript_analyzer"]["input"] 
OUTPUT_PREFIX = config.PREFIXES["transcript_analyzer"]["output"]

def parse_filename(blob_name: str):
    """Parses filenames like 'AAL_2025-06-30.json'."""
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)

def read_transcript_content(raw_json: str) -> str | None:
    """Extracts the 'content' from the raw transcript JSON."""
    try:
        data = json.loads(raw_json)
        # Handle case where data might be a list (older files) or a dict (newer files)
        if isinstance(data, list) and data:
            data = data[0]
        return data.get("content")
    except (json.JSONDecodeError, TypeError, IndexError):
        return None

def process_blob(blob_name: str):
    """
    Processes a single raw transcript file from GCS.
    """
    ticker, date_str = parse_filename(blob_name)
    if not ticker or not date_str:
        return None

    output_blob_name = f"{OUTPUT_PREFIX}{ticker}_{date_str}.json"
    logging.info(f"[{ticker}] Generating transcript analysis for {date_str}")
    
    # 1. Read Raw Content
    raw_json_content = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    if not raw_json_content:
        logging.error(f"[{ticker}] Could not read raw transcript content from {blob_name}")
        return None
        
    transcript_content = read_transcript_content(raw_json_content)
    if not transcript_content:
        logging.error(f"[{ticker}] Could not extract 'content' from {blob_name}")
        return None
    
    # 2. Analyze with Vertex AI
    prompt = r"""
You are a sharp financial analyst evaluating an earnings call transcript to find signals that may influence the stock over the next 1–3 months.
Use **only** the full transcript provided. Your analysis **must** be grounded in the data.

### Key Interpretation Guidelines & Data Integration
1.  **Guidance & Outlook**: Was guidance changed? You **must** cite the specific guidance revision (e.g., 'revising our full year 2025 outlook lower').
2.  **Performance vs. Expectations**: Did the company beat or miss? Cite specific metrics if available (e.g., 'net sales declined 0.6%').
3.  **Tone & Sentiment**: What was management's tone? You **must** include a short, direct quote that captures their sentiment (e.g., 'fall short of our expectations').
4.  **Synthesis**: Combine these data points into a cohesive narrative.
5.  **No Material Signals**: If balanced or neutral, output 0.50.

### CRITICAL FORMATTING RULE
- When including direct quotes in the 'analysis' text, you MUST use single quotes ('), not double quotes ("). This is to ensure the final JSON is clean and renders correctly.

### Step-by-Step Reasoning
1.  Scan the full transcript to identify specific data points and quotes required by the guidelines.
2.  Assess the overall tone and key performance metrics from both prepared remarks and the Q&A section.
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

Provided Transcript:
{{transcript_content}}
""".replace("{{transcript_content}}", transcript_content)

    try:
        analysis_json = vertex_ai.generate(prompt)
        
        # Simple validation
        if "{" not in analysis_json:
            raise ValueError("Model output not JSON")

        gcs.write_text(config.GCS_BUCKET_NAME, output_blob_name, analysis_json, "application/json")
        gcs.cleanup_old_files(config.GCS_BUCKET_NAME, OUTPUT_PREFIX, ticker, output_blob_name)
        
        return output_blob_name

    except Exception as e:
        logging.error(f"[{ticker}] Transcript analysis failed: {e}")
        return None

def run_pipeline():
    """
    Finds and processes new transcript files that have not yet been analyzed.
    """
    logging.info("--- Starting Direct Transcript Analysis Pipeline ---")
    
    # 1. List all available raw transcripts
    all_inputs = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    
    # 2. List all existing analyses
    all_analyses = set(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX))
    
    # 3. Determine work items (Input exists but Output doesn't)
    work_items = [
        blob for blob in all_inputs 
        if f"{OUTPUT_PREFIX}{os.path.basename(blob)}" not in all_analyses
    ]
            
    if not work_items:
        logging.info("All transcripts are already analyzed.")
        return

    logging.info(f"Found {len(work_items)} new transcripts to analyze.")
    
    processed_count = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = {executor.submit(process_blob, blob): blob for blob in work_items}
        
        for future in as_completed(futures):
            if future.result():
                processed_count += 1
        
    logging.info(f"--- Transcript Analysis Pipeline Finished. Processed {processed_count} new files. ---")