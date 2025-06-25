# transcript_summarizer/config.py
import os

# --- GCP Project ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-lx6bb")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "profit-scout-data")

# --- API Keys ---
GEMINI_API_KEY_SECRET = os.getenv("GEMINI_API_KEY_SECRET", "GEMINI_API_KEY")

# --- I/O Folders ---
GCS_INPUT_FOLDER = "earnings-call-transcripts/"
GCS_OUTPUT_FOLDER = "earnings-call-summaries/"

# --- Job Parameters ---
MAX_WORKERS = 3

# --- Generative AI Model Configuration ---
MODEL_NAME = "models/gemini-2.0-flash"
TEMPERATURE = 0.1
MAX_TOKENS = 8192

# --- AI Summarization Prompt ---
TRANSCRIPT_SUMMARY_PROMPT = """
You are a financial analyst summarizing an earnings call transcript for **{ticker}** (Q{quarter} {year}). Your goal is to extract key information and specific words/phrases that signal potential short-term stock price movements (30–90 days), producing a concise summary suitable for embedding as features in predictive models. Focus on precise metrics, strategic initiatives, Q&A sentiment, operational details, liquidity, and sentiment-rich language.

**Instructions**:

**Key Financial Metrics**
- Summarize key financial results, including revenue, EPS, operating margin, and free cash flow.
- Include year-over-year (YoY) growth and comparison to consensus expectations.
- Highlight signal words like ‘beat,’ ‘exceeded,’ or ‘declined.’

**Key Discussion Points**
- Extract 3–5 major events, decisions, or surprises with potential stock price impact.
- Include specifics on strategic initiatives (e.g., scale, timeline).
- Note one key operational highlight and one insightful Q&A exchange.

**Sentiment Tone**
- State the overall tone (Positive, Neutral, Negative) and compare it to prior quarters.
- List 2–3 drivers for the sentiment (e.g., EPS beat, margin pressure).

**Short-Term Outlook**
- Predict the likely stock price impact (30–90 days) based on guidance, Q&A sentiment, and macro factors mentioned in the call.

**Forward-Looking Signals**
- Extract 4–6 forward-looking insights, including guidance, liquidity signals (e.g., cash on hand), and competitive positioning.

**CRITICAL INSTRUCTIONS**:
- Use only the provided transcript for **{ticker}** Q{quarter} {year}.
- Do not invent facts or use external data.
- Output only the structured analysis (the five sections above).
- Keep it concise and precise.

---

**Transcript for {ticker}**:
{transcript_text}
"""