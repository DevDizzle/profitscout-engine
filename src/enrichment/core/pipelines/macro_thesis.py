# src/enrichment/core/pipelines/macro_thesis.py
"""Generate a macro thesis snapshot using Vertex AI grounded Google Search."""

from __future__ import annotations

import datetime as dt
import json
import logging

from google.genai import types

from .. import config, gcs
from ..clients import vertex_ai

_LOG = logging.getLogger(__name__)

GOOGLE_SEARCH_PROMPT = """You are the lead macro strategist for an investment research team. Use the Google Search grounding tool to\nresearch the following macro question and synthesize the most important takeaways. Query: "{query}".\n\nSearch instructions:\n- Use up to {max_sources} authoritative sources that are no more than 2 weeks old.\n- Focus on macroeconomic trends that influence equity, credit, and rate markets globally.\n- Track both the dominant thesis and credible counterpoints.\n\nOutput instructions:\n- Produce valid JSON using only the keys "macro_trend" and "anti_thesis".\n- "macro_trend": 120-180 word synthesis of the dominant macro narrative supported by search findings.\n- "anti_thesis": 80-120 word discussion of material risks or opposing signals.\n- Do not add markdown or extra commentary beyond the JSON response.\n"""

_GOOGLE_SEARCH_TOOLS = [types.Tool(google_search=types.GoogleSearch())]


def _build_prompt() -> str:
    return GOOGLE_SEARCH_PROMPT.format(
        query=config.MACRO_THESIS_SEARCH_QUERY,
        max_sources=config.MACRO_THESIS_MAX_GROUNDED_SOURCES,
    )


def _fallback_thesis(message: str) -> dict:
    return {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "macro_trend": message,
        "anti_thesis": "Macro thesis generation failed; please review the Vertex AI configuration.",
    }


def _distill_macro_thesis() -> dict:
    if not config.MACRO_THESIS_USE_GOOGLE_SEARCH:
        _LOG.error("Vertex AI Google Search grounding is disabled via configuration.")
        return _fallback_thesis(
            "Google Search grounding is disabled, so no macro thesis was generated."
        )

    prompt = _build_prompt()
    try:
        response_text = vertex_ai.generate(
            prompt,
            config_overrides={
                "tools": _GOOGLE_SEARCH_TOOLS,
                "response_mime_type": "application/json",
            },
        )
        parsed = json.loads(response_text)
    except Exception as exc:
        _LOG.error("Vertex AI macro thesis generation failed: %s", exc, exc_info=True)
        return _fallback_thesis(
            "Automated macro thesis generation failed due to a Vertex AI error."
        )

    return {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "macro_trend": parsed.get("macro_trend", ""),
        "anti_thesis": parsed.get("anti_thesis", ""),
    }


def run_pipeline() -> str | None:
    """Execute the macro thesis pipeline and write the output to GCS."""

    _LOG.info("Starting macro thesis pipeline run…")
    thesis = _distill_macro_thesis()

    blob_name = config.macro_thesis_blob_name()
    sources_metadata = [
        {
            "type": "vertex_google_search",
            "query": config.MACRO_THESIS_SEARCH_QUERY,
            "max_sources": config.MACRO_THESIS_MAX_GROUNDED_SOURCES,
            "enabled": config.MACRO_THESIS_USE_GOOGLE_SEARCH,
        }
    ]

    gcs.write_text(
        config.GCS_BUCKET_NAME,
        blob_name,
        json.dumps(
            {
                "macro_trend": thesis.get("macro_trend", ""),
                "anti_thesis": thesis.get("anti_thesis", ""),
                "generated_at": thesis.get("generated_at"),
                "sources": sources_metadata,
            },
            indent=2,
        ),
        "application/json",
    )

    _LOG.info("Macro thesis written to gs://%s/%s", config.GCS_BUCKET_NAME, blob_name)
    return blob_name
