"""
Agent Arena — MCP Client
=========================
Fetches live signal data from the GammaRips MCP server.
Uses direct BigQuery/Firestore queries (same GCP project) for reliability,
with MCP SSE as fallback for external consumers.

For Agent Arena (internal service on same GCP project), we query BQ directly
to avoid SSE connection overhead. The MCP server is for external agents.
"""

import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional

from google.cloud import bigquery, firestore

from config import GCP_PROJECT, BQ_DATASET, MAX_SIGNALS, MIN_SIGNAL_SCORE

logger = logging.getLogger(__name__)

bq_client = bigquery.Client(project=GCP_PROJECT)
fs_client = firestore.Client(project=GCP_PROJECT)


async def get_todays_signals(scan_date: Optional[str] = None) -> list[dict]:
    """
    Fetch today's top enriched signals from BigQuery.
    Returns up to MAX_SIGNALS signals with score >= MIN_SIGNAL_SCORE.
    """
    if not scan_date:
        scan_date = await _get_latest_scan_date()
        if not scan_date:
            logger.error("No scan dates found in BigQuery")
            return []

    query = f"""
        SELECT *
        FROM `{GCP_PROJECT}.{BQ_DATASET}.overnight_signals_enriched`
        WHERE scan_date = @scan_date
          AND score >= @min_score
        ORDER BY score DESC
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("scan_date", "STRING", scan_date),
            bigquery.ScalarQueryParameter("min_score", "INT64", MIN_SIGNAL_SCORE),
            bigquery.ScalarQueryParameter("limit", "INT64", MAX_SIGNALS),
        ]
    )

    results = await asyncio.to_thread(
        _run_query, query, job_config
    )
    
    logger.info(f"Fetched {len(results)} enriched signals for {scan_date}")
    return results


async def get_raw_signals(scan_date: Optional[str] = None, limit: int = 50) -> list[dict]:
    """
    Fetch raw overnight signals (all scores) from BigQuery.
    """
    if not scan_date:
        scan_date = await _get_latest_scan_date()
        if not scan_date:
            return []

    query = f"""
        SELECT *
        FROM `{GCP_PROJECT}.{BQ_DATASET}.overnight_signals`
        WHERE scan_date = @scan_date
        ORDER BY score DESC
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("scan_date", "STRING", scan_date),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )

    return await asyncio.to_thread(_run_query, query, job_config)


async def get_performance_data(days: int = 14) -> dict:
    """
    Fetch recent win/loss performance data for context.
    Returns summary stats agents can reference.
    """
    query = f"""
        SELECT 
            COUNT(*) as total_signals,
            COUNTIF(outcome = 'win') as wins,
            COUNTIF(outcome = 'loss') as losses,
            ROUND(SAFE_DIVIDE(COUNTIF(outcome = 'win'), COUNT(*)) * 100, 1) as win_rate,
            ROUND(AVG(pnl_pct), 2) as avg_return,
            ROUND(AVG(CASE WHEN direction = 'bull' THEN pnl_pct END), 2) as avg_bull_return,
            ROUND(AVG(CASE WHEN direction = 'bear' THEN pnl_pct END), 2) as avg_bear_return
        FROM `{GCP_PROJECT}.{BQ_DATASET}.signal_performance`
        WHERE scan_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("days", "INT64", days),
        ]
    )

    results = await asyncio.to_thread(_run_query, query, job_config)
    return results[0] if results else {}


async def get_daily_report(scan_date: Optional[str] = None) -> Optional[dict]:
    """
    Fetch the daily report from Firestore for additional context.
    """
    try:
        if scan_date:
            doc = await asyncio.to_thread(
                lambda: fs_client.collection("daily_reports").document(scan_date).get()
            )
            if doc.exists:
                return doc.to_dict()
        else:
            # Get most recent
            docs = await asyncio.to_thread(
                lambda: list(
                    fs_client.collection("daily_reports")
                    .order_by("scan_date", direction=firestore.Query.DESCENDING)
                    .limit(1)
                    .stream()
                )
            )
            if docs:
                return docs[0].to_dict()
    except Exception as e:
        logger.warning(f"Failed to fetch daily report: {e}")
    
    return None


def format_signals_for_prompt(signals: list[dict]) -> str:
    """
    Format enriched signals into a readable text block for agent prompts.
    """
    if not signals:
        return "NO SIGNALS AVAILABLE"

    lines = []
    for i, sig in enumerate(signals, 1):
        lines.append(f"--- Signal #{i} ---")
        lines.append(f"Ticker: {sig.get('ticker', 'N/A')}")
        lines.append(f"Direction: {sig.get('direction', 'N/A').upper()}")
        lines.append(f"Overnight Score: {sig.get('score', 'N/A')}/10")
        
        # Raw signal data
        if sig.get('volume'):
            lines.append(f"Volume: {sig['volume']}")
        if sig.get('premium'):
            lines.append(f"Premium: ${sig['premium']:,.0f}" if isinstance(sig.get('premium'), (int, float)) else f"Premium: {sig['premium']}")
        if sig.get('expiration'):
            lines.append(f"Expiration: {sig['expiration']}")
        if sig.get('strike'):
            lines.append(f"Strike: ${sig['strike']}")
        if sig.get('option_type'):
            lines.append(f"Type: {sig['option_type']}")

        # Enriched data
        if sig.get('news_summary'):
            lines.append(f"News: {sig['news_summary']}")
        if sig.get('technical_context'):
            lines.append(f"Technicals: {sig['technical_context']}")
        if sig.get('catalyst_assessment'):
            lines.append(f"Catalyst: {sig['catalyst_assessment']}")
        if sig.get('risk_factors'):
            lines.append(f"Risk Factors: {sig['risk_factors']}")
        if sig.get('analyst_consensus'):
            lines.append(f"Analyst Consensus: {sig['analyst_consensus']}")
        if sig.get('sector'):
            lines.append(f"Sector: {sig['sector']}")
        
        lines.append("")

    return "\n".join(lines)


def format_performance_context(perf: dict) -> str:
    """Format performance data as context for agents."""
    if not perf or not perf.get('total_signals'):
        return "No historical performance data available yet."
    
    return (
        f"Historical Performance (last 14 days): "
        f"{perf.get('total_signals', 0)} signals tracked, "
        f"{perf.get('win_rate', 0)}% win rate, "
        f"{perf.get('avg_return', 0)}% avg return. "
        f"Bull avg: {perf.get('avg_bull_return', 'N/A')}%, "
        f"Bear avg: {perf.get('avg_bear_return', 'N/A')}%."
    )


# ============================================================
# Internal helpers
# ============================================================

async def _get_latest_scan_date() -> Optional[str]:
    """Get the most recent scan date from BigQuery."""
    query = f"""
        SELECT MAX(scan_date) as latest
        FROM `{GCP_PROJECT}.{BQ_DATASET}.overnight_signals_enriched`
    """
    results = await asyncio.to_thread(_run_query, query, None)
    if results and results[0].get("latest"):
        return str(results[0]["latest"])
    return None


def _run_query(query: str, job_config) -> list[dict]:
    """Execute a BigQuery query and return results as list of dicts."""
    try:
        if job_config:
            result = bq_client.query(query, job_config=job_config).result()
        else:
            result = bq_client.query(query).result()
        
        rows = []
        for row in result:
            row_dict = dict(row)
            # Convert non-serializable types
            for k, v in row_dict.items():
                if hasattr(v, 'isoformat'):
                    row_dict[k] = v.isoformat()
                elif isinstance(v, bytes):
                    row_dict[k] = v.decode('utf-8', errors='replace')
            rows.append(row_dict)
        return rows
    except Exception as e:
        logger.error(f"BigQuery error: {e}")
        return []
