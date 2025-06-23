import os, re, json, time, logging, tempfile, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd                        # <- still imported (no functional change)
from google.cloud import storage
from tenacity import (
    retry, stop_after_attempt, wait_exponential, retry_if_exception_type
)
import google.api_core.exceptions as core_exc

# ---- Google GenAI (note: google-genai, NOT google-generativeai) ----
from google import genai
from google.genai.types import GenerateContentConfig, HttpOptions

# ---------- CONFIG ----------
PROJECT_ID               = os.getenv("PROJECT_ID")
GCS_BUCKET_NAME          = os.getenv("GCS_BUCKET_NAME")

GCS_INPUT_PREFIX         = "earnings-call/json/"           # <- JSON transcripts
GCS_OUTPUT_PREFIX        = "earnings-call-summary/"        # <- hot summaries
GCS_OUTPUT_COLD_PREFIX   = "earnings-call-summary-cold/"   # <- >2 yrs archived

MODEL_NAME               = "models/gemini-2.0-flash-001"
TEMPERATURE              = 0.1
MAX_TOKENS               = 8192
GEMINI_API_KEY           = os.getenv("GEMINI_API_KEY")

MAX_THREADS              = 3
YEARS_TO_KEEP            = 2                                # keep last 2 years only

# ---------- PROMPT ----------
TRANSCRIPT_SUMMARY_PROMPT = """
You are a financial analyst summarizing an earnings call transcript for **{ticker}** (Q{quarter} {year}). Your goal is to extract key information and specific words/phrases that signal potential short-term stock price movements (30–90 days), producing a concise summary suitable for embedding as features in predictive models. Focus on precise metrics, strategic initiatives, Q&A sentiment, operational details, liquidity, and sentiment-rich language, generalizable across all Russell 1000 companies (e.g., technology, retail, energy, financials, healthcare).

---

**Instructions**:

**Key Financial Metrics**
- Summarize key financial results for the reported quarter, including revenue, EPS, operating income/margin, free cash flow, and segment/vertical performance (e.g., ‘retail grew 4%’) or category performance (e.g., ‘battery sales strong’) if available.
- Include year-over-year (YoY) growth, comparison to consensus expectations, and exact figures (e.g., $2.37B, +1.3% YoY).
- Highlight signal words (e.g., ‘beat,’ ‘exceeded,’ ‘declined’) in context.
- If no metrics provided, state: ‘No financial metrics provided.’
- Format: ‘[Metric]: [Value, % YoY, vs. consensus if available, with signal words].’
- Example: ‘Revenue: $2.37B, +1.3% YoY, exceeded consensus $2.35B.’

**Key Discussion Points**
- Extract 3–5 major events, decisions, or surprises (e.g., product launches, restructuring, client wins) with potential stock price impact.
- Include one strategic initiative with specifics (e.g., scale, timeline, impact) and signal words (e.g., ‘accelerated,’ ‘transformational’).
- Include one operational highlight (e.g., ‘100% nuclear uptime,’ ‘client retention improved’) with signal words, if available.
- Include one Q&A insight (e.g., analyst reaction, management confidence) with a direct quote (e.g., ‘nice results’), if available.
- Format: ‘[Speaker] – [Event, with specifics and signal words].’
- Example: ‘Analyst – Noted “good progress,” affirming strategic execution.’

**Sentiment Tone**
- State the overall tone (Positive, Neutral, Negative), based on management remarks, Q&A, and signal words (e.g., ‘confident,’ ‘challenges’).
- Compare to prior quarters (e.g., shift from cautious to optimistic), grounding in transcript evidence or inferred context (e.g., ‘post-prior losses,’ ‘post-integration challenges’).
- List 2–3 drivers (e.g., EPS beat, Q&A positivity, margin pressure).
- Example: ‘Positive, shifted from cautious 2023 due to prior losses; driven by EPS beat, confident Q&A remarks.’

**Short-Term Outlook**
- Predict stock price impact (30–90 days) using financials, guidance, strategic moves, Q&A sentiment, liquidity, and macro factors.
- Highlight guidance (e.g., raised, unchanged), macro influences (e.g., tariffs, consumer spending), competitive positioning, and signal words (e.g., ‘robust,’ ‘headwinds’).
- Example: ‘Positive outlook driven by raised EPS guidance and robust client wins, despite FX headwinds.’

**Forward-Looking Signals**
- Extract 4–6 forward-looking insights, including:
  - At least one guidance-related signal (e.g., revenue, EPS, margin) with projected value, comparison to consensus/prior guidance, and signal words (e.g., ‘raised,’ ‘conservative’).
  - One Q&A insight (e.g., analyst positivity, management confidence) with a direct quote (e.g., ‘analysts praised “spectacular year”’), if available.
  - One liquidity or capital allocation signal (e.g., ‘$1.9B cash,’ ‘$1.45B asset sale proceeds’).
  - Additional signals on macro factors, competitive positioning, operational metrics, or one-time events (e.g., ‘asset sale completed’).
- If no guidance, state: ‘No guidance provided’ and include other signals.
- Format: ‘[Topic]: [Description, with specifics and signal words].’
- Example: ‘Liquidity: $1.9B cash from asset sale, bolstering financial strength.’

---

**CRITICAL INSTRUCTIONS**:
- Use only the provided transcript for **{ticker}** Q{quarter} {year}.
- Do not invent facts, use speculation, or reference external data.
- Output only the structured analysis (the five sections above, each as a header followed by bullet points).
- Highlight signal-rich words/phrases naturally.
- Include at least one Q&A insight with a direct quote, one operational detail, one liquidity/capital signal, and prior-quarter sentiment evidence.
- If a section lacks data, state: ‘No [section name] data provided.’
- Keep it concise (200–300 words), precise ($1.9B cash), and generalizable across sectors.
- Optimize for downstream embedding by maximizing signal-rich content and minimizing formatting noise.

---

**Transcript for {ticker}**:
"""

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- CLIENTS ----------
storage_client = storage.Client(project=PROJECT_ID)
bucket         = storage_client.bucket(GCS_BUCKET_NAME)

genai_client = genai.Client(
    api_key=GEMINI_API_KEY,
    http_options=HttpOptions(api_version="v1beta")
)

# ---------- RETRY WRAPPER ----------
@retry(
    retry=retry_if_exception_type((core_exc.InternalServerError,
                                   core_exc.ServiceUnavailable)),
    wait=wait_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def generate_with_retry(prompt: str):
    return genai_client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=GenerateContentConfig(
            temperature=TEMPERATURE,
            max_output_tokens=MAX_TOKENS,
        ),
    )

# ---------- HELPERS ----------
def _load_json_transcript(blob) -> str:
    """Return raw transcript text from JSON object."""
    data = json.loads(blob.download_as_text(encoding="utf-8"))
    # Common field names we’ve seen
    for key in ("transcript", "text", "body"):
        if isinstance(data, dict) and key in data and data[key]:
            return data[key]
    # Fallback: dump entire JSON as text
    return json.dumps(data, ensure_ascii=False)

def summarize_transcript(ticker: str, blob_name: str):
    try:
        blob     = bucket.blob(blob_name)
        text     = _load_json_transcript(blob)

        filename = os.path.basename(blob_name)                      # AAPL_2025-04-23.json
        m        = re.search(r"_([0-9]{4})-([0-9]{2})-([0-9]{2})\.json$", filename)
        if not m:
            logging.error(f"Could not parse date from filename: {filename}")
            return
        year, month = int(m.group(1)), int(m.group(2))
        quarter     = (month - 1) // 3 + 1

        prompt = (
            TRANSCRIPT_SUMMARY_PROMPT.format(ticker=ticker,
                                             quarter=quarter,
                                             year=year)
            + text
            + "\n\n---\n"
        )

        response = generate_with_retry(prompt)
        summary  = response.text or "No response"

        target_blob = f"{GCS_OUTPUT_PREFIX}{filename.replace('.json', '.txt')}"
        with tempfile.NamedTemporaryFile(delete=False,
                                         mode="w",
                                         encoding="utf-8") as tmp:
            tmp.write(summary)
            tmp_path = tmp.name
        bucket.blob(target_blob).upload_from_filename(tmp_path)
        os.remove(tmp_path)

        logging.info(f"Uploaded summary → {target_blob}")
        time.sleep(1)  # gentle throttle
    except Exception as exc:
        logging.error(f"{ticker}: summarization failed – {exc}")

def _archive_old_summaries():
    """Move summaries older than YEARS_TO_KEEP to cold storage folder."""
    cutoff = datetime.date.today() - datetime.timedelta(days=365 * YEARS_TO_KEEP)
    for b in bucket.list_blobs(prefix=GCS_OUTPUT_PREFIX):
        if not b.name.endswith(".txt"):
            continue
        m = re.search(r"_([0-9]{4})-([0-9]{2})-([0-9]{2})\.txt$", b.name)
        if not m:
            continue
        file_date = datetime.date(int(m.group(1)),
                                  int(m.group(2)),
                                  int(m.group(3)))
        if file_date < cutoff:
            cold_name = b.name.replace(GCS_OUTPUT_PREFIX, GCS_OUTPUT_COLD_PREFIX)
            bucket.copy_blob(b, bucket, cold_name)
            b.delete()
            logging.info(f"Archived old summary → {cold_name}")

# ---------- DRIVER ----------
def _run():
    blobs = list(bucket.list_blobs(prefix=GCS_INPUT_PREFIX))

    tasks = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        for b in blobs:
            if not b.name.endswith(".json"):
                continue

            # Parse ticker from filename
            ticker = os.path.basename(b.name).split("_")[0].upper()

            # Skip if summary already exists
            out_blob = b.name.replace(GCS_INPUT_PREFIX,
                                      GCS_OUTPUT_PREFIX).replace(".json", ".txt")
            if bucket.blob(out_blob).exists():
                logging.info(f"{ticker}: summary already exists; skipping")
                continue

            tasks.append(pool.submit(summarize_transcript, ticker, b.name))

        for f in as_completed(tasks):
            f.result()

    # After all processing, archive anything >2 years old
    _archive_old_summaries()
    logging.info("All transcripts processed.")

# ---------- CLOUD FUNCTION ENTRYPOINT ----------
def main(request, context=None):
    _run()
    return "Summaries refreshed", 200
