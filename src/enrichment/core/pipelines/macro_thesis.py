# enrichment/core/pipelines/macro_thesis.py
"""Generate a single macro worldview snapshot using Gemini + Google Search grounding."""

from __future__ import annotations

import datetime as dt
import logging

from .. import config, gcs
from ..clients import vertex_ai

_LOG = logging.getLogger(__name__)

WORLDVIEW_PROMPT = """
You are the lead macro strategist for an investment research team. Your task is to produce an objective, data-driven analysis of the current global macro environment. Your goal is to discover and synthesize the most critical trends, not to confirm preconceived notions.

**MANDATORY: You must perform deep Google Searches to gather specific, up-to-date data for today's date.**

Your research should focus on identifying and evaluating:

1. The Dominant Macro Narrative: What are the prevailing global trends concerning economic growth, inflation, and the current stage of the business cycle? Search for recent PMI readings, GDP prints, and inflation reports.

2. Monetary Policy Divergence: Analyze the policy stances and forward guidance of major central banks (e.g., Fed, ECB, BoJ, PBoC). Search for recent central bank meeting minutes, speeches by governors, and interest rate decisions.

3. Economic Health and Resilience: Assess the condition of global labor markets and consumer health. Search for recent employment data (Non-Farm Payrolls, Jobless Claims), retail sales figures, and consumer confidence indices.

4. Key Risks and Catalysts: Identify the most significant tail risks and potential upside catalysts. Search for geopolitical developments, major policy shifts, or financial stability concerns.

5. Market Sentiment: Characterize the overall risk sentiment. Search for recent performance of major indices (S&P 500, Nasdaq, 10Y Yields, VIX, Dollar Index) and credit spreads.

Write your answer as a single dense paragraph (roughly 180–250 words) suitable for an options trader who buys premium and typically exits after 2–3% moves in the underlying over 1–5 trading days.

You may include citations, numeric markers, or other reference-style notation if helpful.
Do NOT wrap the response in JSON or code fences; just return plain text.
""".strip()


def _generate_worldview() -> dict:
    """Call Gemini with Google Search grounding and capture the raw worldview text.

    Returns:
        {
            "generated_at": <ISO8601 UTC>,
            "worldview": <str>,
        }
    """
    fallback_worldview = (
        "Automated macro worldview generation failed. Treat the current environment as "
        "uncertain and lean on core indicators: growth vs. slowdown (PMIs, ISM, earnings "
        "revisions), inflation and central-bank guidance (Fed, ECB, BOJ, PBoC), labor market "
        "and consumer health (employment, retail sales, delinquencies), credit spreads and "
        "liquidity (IG/HY spreads, funding markets), and cross-asset risk sentiment across "
        "equities, bonds, volatility, the dollar, and key commodities."
    )

    try:
        response_text, _ = vertex_ai.generate_with_tools(
            prompt=WORLDVIEW_PROMPT,
            model_name=getattr(config, "MACRO_THESIS_MODEL_NAME", config.MODEL_NAME),
            temperature=getattr(
                config, "MACRO_THESIS_TEMPERATURE", config.TEMPERATURE
            ),
        )

        if not response_text:
            raise ValueError("Empty response from grounded Gemini call.")

        worldview = response_text.strip()
        if not worldview:
            _LOG.error("Worldview text was empty after stripping; using hardcoded fallback.")
            worldview = fallback_worldview

    except Exception as exc:
        _LOG.error(
            "Vertex AI grounded macro worldview generation failed: %s",
            exc,
            exc_info=True,
        )
        worldview = fallback_worldview

    return {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "worldview": worldview,
    }


def run_pipeline() -> str | None:
    """Execute the macro worldview pipeline and write the output to GCS."""
    _LOG.info(
        "Starting macro worldview pipeline run… model=%s",
        getattr(config, "MACRO_THESIS_MODEL_NAME", config.MODEL_NAME),
    )

    worldview_data = _generate_worldview()
    worldview_text = worldview_data.get("worldview", "")
    if not worldview_text:
        _LOG.error("Generated macro worldview text was empty; aborting write to GCS.")
        return None

    blob_name = config.macro_thesis_blob_name()
    gcs.write_text(
        config.GCS_BUCKET_NAME,
        blob_name,
        worldview_text,
        "text/plain",
    )

    _LOG.info(
        "Macro worldview written to gs://%s/%s (overwriting previous snapshot)",
        config.GCS_BUCKET_NAME,
        blob_name,
    )
    return blob_name
