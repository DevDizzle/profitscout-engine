# serving/core/pipelines/options_recommendation_generator.py
import logging
import re
from datetime import date
from typing import Dict, List

import pandas as pd
from google.cloud import bigquery

from .. import config, gcs
from ..clients import vertex_ai

PROJECT = config.SOURCE_PROJECT_ID if hasattr(config, "SOURCE_PROJECT_ID") else config.PROJECT_ID
DATASET = config.BIGQUERY_DATASET
CAND_TABLE = f"{PROJECT}.{DATASET}.options_candidates"

# Where to write MD artifacts (add this in config.py if you want a dedicated prefix)
OPTIONS_MD_PREFIX = getattr(config, "OPTIONS_MD_PREFIX", "options-recos/")

_PROMPT = r"""
You are an options analyst writing a short, momentum-led options note.

Write clean, concise Markdown. Keep to ≤ 220 words total. No tables.

### Formatting (strict)
- H1: "# {company} ({ticker}) – Options Pick"
- Next line: "**Signal:** {signal}"
- Then: "**Quick take:**" 1–2 sentences.
- "### Selection Criteria" bullet points (4–6 bullets) covering liquidity (spread, OI/volume), moneyness (5–10% OTM aligned with direction), Greeks (moderate |Delta|, responsive Gamma, lower Theta), and IV context.
- "### Finalists (Top 5)" bullets: one line per contract "SYMBOL – STRIKE {type} exp {expiry} | Score={score:.3f}, Δ={delta:.2f}, Γ={gamma:.2f}, Θ={theta:.2f}, IV={iv:.1f}%, OI={oi}, Vol={vol}, Spread={spread:.1%}"
- "### Why these?" 1–2 bullets tying momentum intent to the above metrics.

### Inputs
Ticker: {ticker}
Signal: {signal}
Company: {company}

Candidates (top-5, pre-filtered & scored):
{candidates}

Guidelines:
- Favor liquidity (tight spreads, higher OI/volume). 
- Prefer moderate |Delta| and higher Gamma for responsiveness; penalize high Theta.
- Avoid very high IV percentile when available.
- Do NOT invent data; only use what’s shown.
- Do NOT include calls to action.
- Keep it readable and skimmable.
"""

def _fetch_work(bq: bigquery.Client) -> pd.DataFrame:
    q = f"""
    SELECT *
    FROM `{CAND_TABLE}`
    WHERE fetch_date = CURRENT_DATE("America/New_York")
    ORDER BY ticker, signal, rn
    """
    return bq.query(q).to_dataframe()

def _format_candidates_block(df: pd.DataFrame) -> str:
    rows: List[str] = []
    for _, r in df.iterrows():
        spread = (r["ask"] - r["bid"]) / ((r["bid"] + r["ask"]) / 2) if (r["bid"] + r["ask"]) else None
        rows.append(
            f"- {r['contract_symbol']} – {r['strike']:.2f} {r['option_type']} exp {r['expiration_date']} | "
            f"Score={r['options_score']:.3f}, Δ={r['delta']:.2f}, Γ={r['gamma']:.2f}, Θ={r['theta']:.2f}, "
            f"IV={r['implied_volatility']:.1f}%, OI={int(r['open_interest'])}, Vol={int(r['volume'])}"
            + (f", Spread={spread:.1%}" if spread is not None else "")
        )
    return "\n".join(rows)

def _company_name_for_ticker(bq: bigquery.Client, ticker: str) -> str:
    # Try your metadata table if available; otherwise fallback to ticker.
    table_id = getattr(config, "BUNDLER_STOCK_METADATA_TABLE_ID", None)
    if not table_id:
        return ticker
    q = f"""
    WITH Latest AS (
      SELECT ticker, company_name, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY quarter_end_date DESC) rn
      FROM `{table_id}`
    )
    SELECT company_name FROM Latest WHERE ticker=@t AND rn=1
    """
    df = bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("t", "STRING", ticker)]
    )).to_dataframe()
    return df["company_name"].iloc[0] if not df.empty else ticker

def _render_one(bq: bigquery.Client, ticker: str, signal: str, group: pd.DataFrame) -> str:
    company = _company_name_for_ticker(bq, ticker)
    cblock = _format_candidates_block(group)
    prompt = _PROMPT.format(
        company=company,
        ticker=ticker,
        signal=signal,
        candidates=cblock
    )
    md = vertex_ai.generate(prompt)
    # Keep only markdown-like content
    md = md.strip()
    return md

def _write_md(ticker: str, md: str):
    today_str = date.today().strftime("%Y-%m-%d")
    blob_path = f"{OPTIONS_MD_PREFIX}{ticker}_options_{today_str}.md"
    gcs.write_text(config.GCS_BUCKET_NAME, blob_path, md, "text/markdown")
    logging.info("[%s] wrote %s", ticker, blob_path)
    return blob_path

def run_pipeline(bq_client: bigquery.Client | None = None):
    logging.info("--- Starting Options Options Explainer (serving) ---")
    bq = bq_client or bigquery.Client(project=PROJECT)

    df = _fetch_work(bq)
    if df.empty:
        logging.warning("No candidates found for today; aborting.")
        return

    # process per (ticker, signal)
    for (ticker, signal), group in df.groupby(["ticker", "signal"]):
        try:
            md = _render_one(bq, ticker, signal, group)
            _write_md(ticker, md)
        except Exception as e:
            logging.error("[%s/%s] rendering failed: %s", ticker, signal, e, exc_info=True)

    logging.info("--- Options Options Explainer Finished ---")
