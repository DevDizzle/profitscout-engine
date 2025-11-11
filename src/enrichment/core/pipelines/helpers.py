"""Shared helpers for enrichment pipelines."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict

from .. import config, gcs

_LOG = logging.getLogger(__name__)


def load_latest_macro_thesis() -> Dict[str, str]:
    """Return the latest macro thesis document if it exists.

    Falls back to empty strings when no thesis is available so downstream prompts
    remain well-formed regardless of pipeline freshness.
    """

    blob_name = config.get_latest_macro_thesis_blob()
    if not blob_name:
        return {"macro_trend": "", "anti_thesis": ""}

    try:
        raw = gcs.read_blob(config.GCS_BUCKET_NAME, blob_name)
    except Exception as exc:  # pragma: no cover - network failures / transient issues
        _LOG.warning("Unable to read macro thesis blob %s: %s", blob_name, exc)
        return {"macro_trend": "", "anti_thesis": ""}

    try:
        parsed: Dict[str, Any] = json.loads(raw or "{}")
    except json.JSONDecodeError:
        _LOG.warning("Macro thesis blob %s contained invalid JSON", blob_name)
        return {"macro_trend": "", "anti_thesis": ""}

    macro_trend = str(parsed.get("macro_trend") or "").strip()
    anti_thesis = str(parsed.get("anti_thesis") or "").strip()
    return {"macro_trend": macro_trend, "anti_thesis": anti_thesis}
