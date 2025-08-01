import json
import logging
import os
import textwrap
from datetime import datetime
from typing import List

import langextract as lx
from google.generativeai import configure as genai_configure

from . import client, config

_log = logging.getLogger(__name__)

# Configure GenAI for LangExtract (assumes GOOGLE_API_KEY env var is set in Cloud Function; set it via Google Cloud Console)
genai_configure(api_key=os.getenv("GOOGLE_API_KEY"))

# Define extraction configs for each section (prompts + few-shot examples)
# Note: Examples are placeholders; tune with real samples from past transcripts for better accuracy

METRICS_PROMPT = textwrap.dedent("""
    Extract key financial metrics like revenue, EPS, margins, FCF, segments from the transcript.
    Use exact text spans. Provide attributes for type, value, YoY change, vs. consensus, and surprises (beat/miss).
""")

METRICS_EXAMPLES: List[lx.data.ExampleData] = [
    lx.data.ExampleData(
        text="Revenue was $10B, up 5% YoY, beating estimates by 2%. EPS of $1.20 exceeded guidance.",
        extractions=[
            lx.data.Extraction(
                extraction_class="metric",
                extraction_text="Revenue was $10B",
                attributes={"type": "revenue", "value": "10B", "yoy": "5%", "surprise": "beat"}
            ),
            lx.data.Extraction(
                extraction_class="metric",
                extraction_text="EPS of $1.20",
                attributes={"type": "eps", "value": "1.20", "surprise": "exceeded"}
            ),
        ]
    )
    # Add 1-2 more examples for better few-shot performance
]

DISCUSSION_PROMPT = textwrap.dedent("""
    Extract 3-5 key events, strategies, operations, or Q&A insights with quotes.
    Provide attributes for topic, impact (positive/negative), and quotes.
""")

DISCUSSION_EXAMPLES: List[lx.data.ExampleData] = [
    lx.data.ExampleData(
        text="Management highlighted strong demand in premium cabins. 'We saw robust growth,' said the CEO.",
        extractions=[
            lx.data.Extraction(
                extraction_class="discussion_point",
                extraction_text="'We saw robust growth,' said the CEO.",
                attributes={"topic": "demand", "impact": "positive", "quote": "We saw robust growth"}
            ),
        ]
    )
]

SENTIMENT_PROMPT = textwrap.dedent("""
    Extract signal words/phrases indicating tone (positive/neutral/negative).
    Provide attributes for polarity, drivers, and comparison to prior quarters.
""")

SENTIMENT_EXAMPLES: List[lx.data.ExampleData] = [
    lx.data.ExampleData(
        text="The quarter was challenging but we're optimistic about recovery.",
        extractions=[
            lx.data.Extraction(
                extraction_class="sentiment",
                extraction_text="challenging",
                attributes={"polarity": "negative", "driver": "demand weakness"}
            ),
            lx.data.Extraction(
                extraction_class="sentiment",
                extraction_text="optimistic about recovery",
                attributes={"polarity": "positive", "driver": "future outlook"}
            ),
        ]
    )
]

OUTLOOK_PROMPT = textwrap.dedent("""
    Extract guidance, projections, and short-term impacts (30-90 days).
    Provide attributes for timeframe, change (up/down), and values.
""")

OUTLOOK_EXAMPLES: List[lx.data.ExampleData] = [
    lx.data.ExampleData(
        text="We expect Q3 revenue to grow 3-5%.",
        extractions=[
            lx.data.Extraction(
                extraction_class="outlook",
                extraction_text="expect Q3 revenue to grow 3-5%",
                attributes={"timeframe": "Q3", "change": "up", "value": "3-5%"}
            ),
        ]
    )
]

FORWARD_SIGNALS_PROMPT = textwrap.dedent("""
    Extract 4-6 forward-looking insights like guidance, liquidity, macro factors with values/quotes.
""")

FORWARD_SIGNALS_EXAMPLES: List[lx.data.ExampleData] = [
    lx.data.ExampleData(
        text="Guidance raised due to strong bookings. Liquidity at $5B.",
        extractions=[
            lx.data.Extraction(
                extraction_class="signal",
                extraction_text="Guidance raised",
                attributes={"type": "guidance", "quote": "strong bookings"}
            ),
        ]
    )
]

QA_PROMPT = textwrap.dedent("""
    Extract 2-4 key Q&A pairs, focusing on questions, responses, tone (confident/defensive), and quotes.
    Look for sections after 'Operator' or 'Q&A' markers with speaker changes.
""")

QA_EXAMPLES: List[lx.data.ExampleData] = [
    lx.data.ExampleData(
        text="Analyst: How is demand? CEO: Strong, we're confident.",
        extractions=[
            lx.data.Extraction(
                extraction_class="qa_pair",
                extraction_text="How is demand? CEO: Strong, we're confident.",
                attributes={"question": "How is demand?", "response": "Strong, we're confident.", "tone": "confident"}
            ),
        ]
    )
]

# Helper to run extraction for a section
def run_extraction(transcript: str, prompt: str, examples: List[lx.data.ExampleData]) -> lx.data.AnnotatedDocument:
    return lx.extract(
        text_or_documents=transcript,
        prompt_description=prompt,
        examples=examples,
        model_id=config.LANGEXTRACT_MODEL_ID,
        extraction_passes=2,  # Multiple passes for better recall on long docs
        max_workers=config.MAX_WORKERS,  # Parallel processing
        max_char_buffer=2000,  # Adjust based on transcript length
    )

# Helper to synthesize paragraph from extractions (using LLM for dense narrative)
def synthesize_paragraph(extractions: List[lx.data.Extraction], section_name: str) -> str:
    if not extractions:
        return f"No {section_name.lower()} data provided in the transcript."

    extraction_json = json.dumps([{
        "class": e.extraction_class,
        "text": e.extraction_text,
        "attributes": e.attributes
    } for e in extractions])

    synth_prompt = textwrap.dedent(f"""
        Condense these extracted {section_name.lower()} into 1-2 dense narrative paragraphs with metrics/quotes integrated naturally.
        Use full sentences, no bullets. Maximize density for ML embeddings. Ground in evidence.
        End with 'Topic keywords: keyword1, keyword2, ...' derived from attributes/topics.
        Extractions: {extraction_json}
    """)

    return client.generate(synth_prompt)

def summarise(transcript: str, ticker: str, year: int, quarter: int) -> str:
    """Generates a summary using LangExtract for structured extraction + synthesis."""
    if not all([transcript, ticker, year, quarter]):
        raise ValueError("Transcript, ticker, year, and quarter are all required.")

    # Run extractions for each section
    _log.info("Running LangExtract for each section...")
    metrics_result = run_extraction(transcript, METRICS_PROMPT, METRICS_EXAMPLES)
    discussion_result = run_extraction(transcript, DISCUSSION_PROMPT, DISCUSSION_EXAMPLES)
    sentiment_result = run_extraction(transcript, SENTIMENT_PROMPT, SENTIMENT_EXAMPLES)
    outlook_result = run_extraction(transcript, OUTLOOK_PROMPT, OUTLOOK_EXAMPLES)
    signals_result = run_extraction(transcript, FORWARD_SIGNALS_PROMPT, FORWARD_SIGNALS_EXAMPLES)
    qa_result = run_extraction(transcript, QA_PROMPT, QA_EXAMPLES)

    # Synthesize paragraphs
    _log.info("Synthesizing paragraphs from extractions...")
    metrics_para = synthesize_paragraph(metrics_result.extractions, "Key Financial Metrics")
    discussion_para = synthesize_paragraph(discussion_result.extractions, "Key Discussion Points")
    sentiment_para = synthesize_paragraph(sentiment_result.extractions, "Sentiment Tone")
    outlook_para = synthesize_paragraph(outlook_result.extractions, "Short-Term Outlook")
    signals_para = synthesize_paragraph(signals_result.extractions, "Forward-Looking Signals")
    qa_para = synthesize_paragraph(qa_result.extractions, "Q&A Summary")

    # Concatenate into final summary
    final_summary = f"""
**Key Financial Metrics**
{metrics_para}

**Key Discussion Points**
{discussion_para}

**Sentiment Tone**
{sentiment_para}

**Short-Term Outlook**
{outlook_para}

**Forward-Looking Signals**
{signals_para}

**Q&A Summary**
{qa_para}
    """.strip()

    # Optional: Save intermediate JSONL for debugging (e.g., upload to GCS if desired)
    # lx.io.save_annotated_documents([metrics_result, ...], "temp_extractions.jsonl")

    return final_summary