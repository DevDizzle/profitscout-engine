# src/enrichment/core/pipelines/macro_thesis.py
"""Generate a macro thesis snapshot from configured data sources."""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass
from typing import Iterable, List

import requests

from .. import config, gcs
from ..clients import vertex_ai

_LOG = logging.getLogger(__name__)


@dataclass
class MacroSource:
    name: str
    url: str
    method: str = "GET"
    format: str = "json"
    headers: dict | None = None
    params: dict | None = None


def _normalize_sources(raw_sources: Iterable[dict]) -> list[MacroSource]:
    normalized: list[MacroSource] = []
    for raw in raw_sources:
        try:
            name = (raw.get("name") or raw.get("url") or "macro-source").strip()
            url = raw.get("url", "").strip()
            if not url:
                continue
            normalized.append(
                MacroSource(
                    name=name,
                    url=url,
                    method=(raw.get("method") or "GET").upper(),
                    format=(raw.get("format") or "json").lower(),
                    headers=raw.get("headers"),
                    params=raw.get("params"),
                )
            )
        except Exception as exc:  # pragma: no cover - defensive
            _LOG.warning("Skipping malformed macro source config %s: %s", raw, exc)
    return normalized[: config.MACRO_THESIS_MAX_SOURCES]


def _fetch_source(source: MacroSource) -> dict | None:
    try:
        response = requests.request(
            method=source.method,
            url=source.url,
            headers=source.headers,
            params=source.params,
            timeout=config.MACRO_THESIS_HTTP_TIMEOUT,
        )
        response.raise_for_status()

        if source.format == "json":
            try:
                payload = response.json()
                content = json.dumps(payload, indent=2)
            except ValueError:
                content = response.text
        else:
            content = response.text

        truncated = content[: config.MACRO_THESIS_SOURCE_CHAR_LIMIT]
        return {
            "name": source.name,
            "url": source.url,
            "content": truncated,
        }
    except Exception as exc:
        _LOG.error("Failed to fetch macro source '%s' (%s): %s", source.name, source.url, exc)
        return None


def _collect_sources() -> list[dict]:
    sources = _normalize_sources(config.MACRO_THESIS_SOURCES)
    collected: list[dict] = []
    for source in sources:
        payload = _fetch_source(source)
        if payload:
            collected.append(payload)
    return collected


PROMPT_TEMPLATE = """You are the lead macro strategist for an investment research team. Review the latest macro data below and
produce a JSON document with the following keys:

- "macro_trend": A concise narrative (120-180 words) of the dominant macro trends supported by the data.
- "anti_thesis": A concise narrative (80-120 words) outlining risks or opposing signals investors should monitor.
- "citations": An array of citation objects. Each citation must contain "title" and "url" referencing the sources
  provided.

The response **must** be valid JSON, use double quotes, and avoid markdown. Do not add extra keys.

Sources:
{sources}
"""


def _build_prompt(collected_sources: List[dict]) -> str:
    source_blocks = []
    for idx, item in enumerate(collected_sources, start=1):
        block = (
            f"[Source {idx}] {item['name']}\n"
            f"URL: {item['url']}\n"
            f"Excerpt:\n{item['content']}"
        )
        source_blocks.append(block)
    joined_sources = "\n\n".join(source_blocks) if source_blocks else "(no sources available)"
    return PROMPT_TEMPLATE.format(sources=joined_sources)


def _distill_macro_thesis(collected_sources: List[dict]) -> dict:
    if not collected_sources:
        return {
            "generated_at": dt.datetime.utcnow().isoformat() + "Z",
            "macro_trend": "Macro data sources were unavailable; no thesis generated.",
            "anti_thesis": "The macro thesis could not be generated because no upstream data was retrieved.",
            "citations": [],
        }

    prompt = _build_prompt(collected_sources)
    try:
        response_text = vertex_ai.generate(prompt)
        parsed = json.loads(response_text)
    except Exception as exc:
        _LOG.error("Vertex AI macro thesis generation failed: %s", exc, exc_info=True)
        parsed = {
            "macro_trend": "Automated generation failed. Review the raw macro sources for context.",
            "anti_thesis": "Without a generated thesis, macro risk assessment is inconclusive.",
            "citations": [
                {"title": item["name"], "url": item["url"]}
                for item in collected_sources
            ],
        }

    if "citations" not in parsed or not isinstance(parsed.get("citations"), list):
        parsed["citations"] = []

    normalized_citations = []
    for item in parsed["citations"]:
        if isinstance(item, dict):
            title = item.get("title") or item.get("name")
            url = item.get("url")
        else:
            title = str(item)
            url = next((src["url"] for src in collected_sources if src["name"] == title), "")
        if url:
            normalized_citations.append({"title": title or url, "url": url})

    if not normalized_citations:
        normalized_citations = [
            {"title": item["name"], "url": item["url"]}
            for item in collected_sources
        ]

    return {
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "macro_trend": parsed.get("macro_trend", ""),
        "anti_thesis": parsed.get("anti_thesis", ""),
        "citations": normalized_citations,
    }


def run_pipeline() -> str | None:
    """Execute the macro thesis pipeline and write the output to GCS."""

    _LOG.info("Starting macro thesis pipeline run…")
    collected_sources = _collect_sources()
    _LOG.info("Collected %d macro sources", len(collected_sources))

    thesis = _distill_macro_thesis(collected_sources)
    blob_name = config.macro_thesis_blob_name()
    gcs.write_text(
        config.GCS_BUCKET_NAME,
        blob_name,
        json.dumps({
            "macro_trend": thesis.get("macro_trend", ""),
            "anti_thesis": thesis.get("anti_thesis", ""),
            "citations": thesis.get("citations", []),
            "generated_at": thesis.get("generated_at"),
            "sources": collected_sources,
        }, indent=2),
        "application/json",
    )

    _LOG.info("Macro thesis written to gs://%s/%s", config.GCS_BUCKET_NAME, blob_name)
    return blob_name
