import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from core import config, gcs
from core.clients import vertex_ai
import os
import re
import json

INPUT_PREFIX = config.PREFIXES["transcript_summarizer"]["input"]         # "earnings-call-transcripts/"
OUTPUT_PREFIX = config.PREFIXES["transcript_summarizer"]["output"]       # "earnings-call-summaries/"

def parse_filename(blob_name: str):
    # Expect: TICKER_YYYY-MM-DD.json
    pattern = re.compile(r"([A-Z.]+)_(\d{4}-\d{2}-\d{2})\.json$")
    match = pattern.search(os.path.basename(blob_name))
    return (match.group(1), match.group(2)) if match else (None, None)

def read_transcript_data(raw_json: str):
    try:
        data = json.loads(raw_json)
        if isinstance(data, list) and data:
            data = data[0]
        return data.get("content"), data.get("year"), data.get("quarter")
    except (json.JSONDecodeError, TypeError, ValueError, IndexError):
        return None, None, None

def process_blob(blob_name: str):
    ticker, date_str = parse_filename(blob_name)
    if not ticker or not date_str:
        logging.warning("Skipping blob with unexpected name format: %s", blob_name)
        return None

    summary_blob_path = f"{OUTPUT_PREFIX}{ticker}_{date_str}.txt"
    logging.info("[%s] Generating transcript summary for %s", ticker, date_str)

    raw_json = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    content, year, quarter = read_transcript_data(raw_json)
    if not all([content, year, quarter]):
        logging.error("[%s] Missing content/year/quarter in %s; skipping.", ticker, blob_name)
        return None

    transcript_prompt = r"""You are a financial analyst summarizing an earnings call transcript for {ticker} (Q{quarter} {year}). Use chain-of-thought reasoning to extract key information, surprises, and words/phrases signaling short-term stock movements (30–90 days), producing a single dense, narrative summary optimized for FinBERT embeddings and NLP tasks (LDA). Generalize for Russell 1000 sectors. First, identify financial surprises (beats/misses), guidance updates, tone variations vs. prior quarters, and Q&A interactions (analyst probes, management replies). Second, extract 20-30 topic keywords overall (e.g., 'growth acceleration, margin pressure, demand surge'). Third, condense all elements into a coherent, unified narrative summary in full sentences and paragraphs, maximizing density and signal richness (no bullets, no section headers, integrate metrics/quotes naturally).Reasoning Steps:Financial Surprises and Metrics: Scan for core metrics like revenue, EPS, margins, FCF, and segments; note YoY changes, vs. consensus, and surprises with exact figures and signal phrases (e.g., 'surpassed expectations by 5%,' 'missed guidance due to supply issues'). Highlight implications for momentum.
Guidance and Forward Signals: Extract updates to outlook, projections, or macro influences; include specific values/quotes (e.g., 'raising full-year revenue guidance to $X billion amid robust demand'). Assess changes from prior calls and potential stock catalysts.
Tone and Sentiment Analysis: Evaluate overall sentiment (positive/neutral/negative) across prepared remarks and Q&A; compare to previous quarters, identify drivers like confident language ('excited about pipeline') or cautionary notes ('headwinds persisting'). Incorporate FinBERT-friendly phrases to amplify sentiment cues.
Key Discussions and Strategies: Pull 4-6 operational insights, events, or strategies with embedded quotes (e.g., 'launched new product line driving 15% growth'); focus on elements signaling upside or risks, using financially charged language.
Q&A Dynamics: Locate Q&A (via 'Operator,' speaker shifts); extract 3-5 key exchanges, emphasizing management tone (defensive/confident), analyst sentiment, and unscripted reveals (e.g., 'analyst praised "impressive turnaround," management affirmed "no further delays"').
Signal Synthesis for Stock Movement: Infer aggregated signals for short-term price probability (e.g., beats + raised guidance = positive momentum); weave in phrases indicating volatility or stability, optimizing for embedding capture of predictive signals.

Final Output Instructions:Synthesize all reasoning into one continuous, dense narrative summary (approximately 800-1,000 words to balance density and completeness).
Structure as a flowing story: begin with financial performance and surprises, transition to discussions and tone, incorporate outlook and signals, conclude with Q&A insights and overall implications.
Integrate signal words/quotes naturally (e.g., 'Executives expressed optimism over "accelerating adoption," bolstering raised EPS guidance').
End the summary with a single list: 'Topic keywords: keyword1, keyword2, ...'.
Use only the transcript JSON for {ticker} Q{quarter} {year} (e.g., content arrays with speaker labels).
Do not invent facts; ground in evidence.
If data missing, note briefly (e.g., 'Guidance unchanged, though tone upbeat').
Optimize for FinBERT embeddings: Dense, signal-rich text with financial jargon, sentiment cues, predictive phrases, and balanced positive/negative indicators.
Prioritize unscripted elements in Q&A for authentic signals.

Transcript:
{transcript}
""".format(
        ticker=ticker,
        quarter=quarter,
        year=year,
        transcript=content
    )

    # Generate + persist
    summary_text = vertex_ai.generate(transcript_prompt)
    gcs.write_text(config.GCS_BUCKET_NAME, summary_blob_path, summary_text)

    # IMPORTANT: No cleanup here.
    return summary_blob_path

def run_pipeline():
    logging.info("--- Starting Transcript Summarizer Pipeline ---")

    # 1) List all input transcripts and all existing summaries
    all_input_paths = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=INPUT_PREFIX)
    all_summary_paths = set(gcs.list_blobs(config.GCS_BUCKET_NAME, prefix=OUTPUT_PREFIX))

    # 2) Compute expected summary names from inputs
    expected_summaries = {
        f"{OUTPUT_PREFIX}{os.path.basename(p).replace('.json', '.txt')}"
        for p in all_input_paths
        if os.path.basename(p).endswith(".json")
    }

    # 3) Work set = expected - existing
    work_items = [
        # store the original input blob path (we need it in process_blob)
        inp for inp in all_input_paths
        if f"{OUTPUT_PREFIX}{os.path.basename(inp).replace('.json', '.txt')}" not in all_summary_paths
    ]

    logging.info(
        "Inputs=%d | Existing summaries=%d | Expected summaries=%d | Missing=%d",
        len(all_input_paths), len(all_summary_paths), len(expected_summaries), len(work_items)
    )

    if work_items:
        logging.info("First 5 missing: %s", [os.path.basename(x) for x in work_items[:5]])

    if not work_items:
        logging.info("All transcript summaries are up-to-date.")
        return

    # 4) Process missing items with bounded concurrency and resilient loop
    processed = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(process_blob, item) for item in work_items]
        for f in as_completed(futures):
            try:
                if f.result():
                    processed += 1
            except Exception as e:
                logging.error("Worker failed: %s", e, exc_info=True)

    logging.info("--- Transcript Summarizer Finished. Processed %d new files. ---", processed)
