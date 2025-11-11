import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from .. import config, gcs
from ..clients import vertex_ai
from datetime import date, datetime
import re
import json

# --- Example Output ---
_EXAMPLE_OUTPUT = """
# American Airlines Group Inc. (AAL) ⚖️

**Outlook:** Neutral / Mixed

**Business snapshot:** A major network air carrier providing scheduled air and cargo transportation to over 350 destinations globally.

## Trade Rationale
- **Bearish Technicals:** Price is below the 21, 50, and 200-day moving averages, confirming a strong downtrend.
- **Weak Momentum:** A negative MACD and an RSI of 45.14 reflect increasing selling pressure, suggesting potential for further declines.
- **Mixed Fundamentals:** While the CFO projects a possible full-year profit, a Q3 loss is anticipated, creating uncertainty.
- **Positive Sector Sentiment:** Bullish holiday travel expectations could provide an industry-wide tailwind, countering some of the stock-specific weakness.

## Near-Term Catalysts
- **Holiday Travel Season:** Increased consumer spending on travel could provide a moderately bullish lift.
- **Q3 Earnings Release:** Guidance and results will be a key driver of near-term price action.

## Key Levels & Timing
- **Support:** None apparent.
- **Resistance:** $11.98 (21-day EMA), $12.35 (50-day SMA).

## Risks & Invalidation
- A sustained break above key moving averages would invalidate the bearish technical outlook.
- Negative news regarding holiday travel demand could remove sector support.

## The Bottom Line
AAL presents a mixed setup. The technical picture is clearly bearish, but positive sector sentiment for holiday travel creates a conflicting signal. The trade lacks a clear directional bias until the price can break key resistance or sector sentiment falters.
"""

# --- Prompt Template (short-term stock trading focused) ---
_PROMPT_TEMPLATE = r"""
You are writing a concise trading brief for a short-term stock trader (swing / position, 1–8 weeks).
Use only facts present in `Aggregated Analysis Text`. Avoid fluff and generic “good vs bad” narratives.
Explicitly surface forward-looking statements, management guidance, or outlook commentary when available. If none exist, state that.

### Output Format (strict)

# {company_name} ({ticker}) {signal_emoji}

**Outlook:** {outlook_signal} {momentum_context}

**Business snapshot:**
A tight summary of what the company does and where it operates.

## Macro Backdrop
- Summarize how the current macro trend ({macro_trend}) and the anti-thesis ({anti_thesis}) reinforce or challenge the trade idea.

## Trade Rationale (2-4 bullets)
- Each bullet MUST be a direct statement combining a theme, specific data, and its immediate impact on price action.
- Prefer quantified details: levels, deltas, SMA/EMA alignment, RSI/MACD state, margin/EBITDA changes, revenue growth, and cash flow notes.

## Near-Term Catalysts (max 3)
- Event → Expected market sensitivity (directional bias or volatility risk).

## Key Levels & Timing
- Support / resistance zones and any notable gaps.
- Suggested holding window (e.g., “~2-6 weeks”) **only if justified** by clear evidence in the source.

## Risks & Invalidation
- The 1–3 most relevant failure conditions for the trade setup.

## 90-Day Options Strategy (JSON)
Return a JSON array in a ```json fenced code block``` with 1–2 recommendation objects. Each object must include the keys:
- `position_type` ("Call" or "Put"),
- `strike` (include price and ITM/ATM/OTM context),
- `timeframe_days` (set to 90),
- `rationale` (tie back to thesis and forward-looking commentary),
- `momentum_signal` (summarize price/technical momentum),
- `volatility_context` (note implied vs historical volatility or catalysts),
- `macro_alignment` (state how macro trend vs anti-thesis supports/risks the contract).
Ensure the JSON contains real values (no placeholders) and is valid.

## The Bottom Line
One short paragraph: synthesize why the setup, catalysts, and levels may favor a short-term trade, and what would invalidate it quickly.

### Rules (strict)
1) **Be specific and concise**: Quote concrete numbers exactly (e.g., “Revenue +16% YoY to $8.5B”, “RSI 58.5”).
2) **Explain impact**: Directly state the "so what" for each data point within the bullet.
3) **Reference forward-looking commentary**: Cite management guidance, outlook, or scenario planning verbatim when present; otherwise state "No forward-looking statements cited."
4) **Omit if absent**: If data for a section isn't in `Aggregated Analysis Text`, omit that section—do NOT invent data.
5) **No advice / no promises**: Use an informational tone with words like “may/could/suggests”.
6) **No options jargon beyond JSON**: Keep narrative stock-focused; reserve options-specific language for the JSON block.

### Input Data
- **Outlook Signal**: {outlook_signal}
- **Momentum Context**: {momentum_context}
- **Macro Trend**: {macro_trend}
- **Macro Anti-Thesis**: {anti_thesis}
- **Aggregated Analysis Text**:
{aggregated_text}

### Signal → Emoji map (internal)
- Strongly Bullish: 🚀
- Moderately Bullish: ⬆️
- Neutral / Mixed: ⚖️
- Moderately Bearish: ⬇️
- Strongly Bearish: 🧨

### Example Output (style only; do not copy content)
{example_output}
"""
def _get_signal_and_context(score: float, momentum_pct: float | None) -> tuple[str, str]:
    """
    Determines the 5-tier outlook signal and the momentum context.
    """
    # --- THIS IS THE FIX ---
    # The scoring ranges have been adjusted to prevent any gaps.
    if score >= 0.75:
        outlook = "Strongly Bullish"
    elif 0.60 <= score < 0.75:
        outlook = "Moderately Bullish"
    elif 0.40 <= score < 0.60:
        outlook = "Neutral / Mixed"
    elif 0.25 <= score < 0.40:
        outlook = "Moderately Bearish"
    else: # score < 0.25
        outlook = "Strongly Bearish"


    context = ""
    if momentum_pct is not None:
        is_bullish_outlook = "Bullish" in outlook
        is_bearish_outlook = "Bearish" in outlook

        if is_bullish_outlook and momentum_pct > 0:
            context = "with confirming positive momentum."
        elif is_bullish_outlook and momentum_pct < 0:
            context = "encountering short-term weakness."
        elif is_bearish_outlook and momentum_pct < 0:
            context = "with confirming negative momentum."
        elif is_bearish_outlook and momentum_pct > 0:
            context = "encountering a short-term rally."

    return outlook, context


def _get_daily_work_list() -> list[dict]:
    """
    MODIFIED: Builds the work list from the GCS tickerlist.txt and enriches it
    with the latest available data from BigQuery for each ticker.
    This version now handles potential duplicate entries by selecting the one with the highest weighted_score.
    """
    logging.info("Fetching work list from GCS and enriching from BigQuery...")
    tickers = gcs.get_tickers()
    if not tickers:
        logging.critical("Ticker list from GCS is empty. No work to do.")
        return []
        
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    
    # The query now de-duplicates the data by selecting the record with the highest
    # `weighted_score` for each ticker on the most recent run date.
    query = f"""
        WITH GCS_Tickers AS (
            SELECT ticker FROM UNNEST(@tickers) AS ticker
        ),
        RankedScores AS (
            SELECT
                t1.ticker,
                t2.company_name,
                t1.weighted_score,
                t1.aggregated_text,
                t1.macro_trend,
                t1.anti_thesis,
                t1.macro_generated_at,
                ROW_NUMBER() OVER(PARTITION BY t1.ticker ORDER BY t1.run_date DESC, t1.weighted_score DESC) as rn
            FROM `{config.SCORES_TABLE_ID}` AS t1
            JOIN `{config.BUNDLER_STOCK_METADATA_TABLE_ID}` AS t2 ON t1.ticker = t2.ticker
            WHERE t1.weighted_score IS NOT NULL AND t2.company_name IS NOT NULL
        ),
        LatestScores AS (
            SELECT * FROM RankedScores WHERE rn = 1
        ),
        LatestMomentum AS (
            SELECT
                ticker,
                close_30d_delta_pct
            FROM (
                SELECT
                    ticker,
                    close_30d_delta_pct,
                    ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY date DESC) as rn
                FROM `{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_analysis_input`
                WHERE close_30d_delta_pct IS NOT NULL
            )
            WHERE rn = 1
        )
        SELECT
            g.ticker,
            s.company_name,
            s.weighted_score,
            s.aggregated_text,
            s.macro_trend,
            s.anti_thesis,
            s.macro_generated_at,
            m.close_30d_delta_pct
        FROM GCS_Tickers g
        LEFT JOIN LatestScores s ON g.ticker = s.ticker
        LEFT JOIN LatestMomentum m ON g.ticker = m.ticker
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("tickers", "STRING", tickers),
        ]
    )

    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        df.dropna(subset=['company_name', 'weighted_score', 'aggregated_text'], inplace=True)
        if df.empty:
            logging.warning("No tickers with sufficient data found after enriching from BigQuery.")
            return []
        logging.info(f"Successfully created work list for {len(df)} tickers.")
        return df.to_dict('records')
    except Exception as e:
        logging.critical(f"Failed to build and enrich work list: {e}", exc_info=True)
        return []


def _delete_old_recommendation_files(ticker: str):
    """Deletes all old recommendation files for a given ticker."""
    prefix = f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation_"
    blobs_to_delete = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix)
    for blob_name in blobs_to_delete:
        try:
            gcs.delete_blob(config.GCS_BUCKET_NAME, blob_name)
        except Exception as e:
            logging.error(f"[{ticker}] Failed to delete old file {blob_name}: {e}")

def _process_ticker(ticker_data: dict):
    """
    Generates recommendation markdown and its companion JSON metadata file.
    """
    ticker = ticker_data["ticker"]
    today_str = date.today().strftime('%Y-%m-%d')
    
    base_blob_path = f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation_{today_str}"
    md_blob_path = f"{base_blob_path}.md"
    json_blob_path = f"{base_blob_path}.json"
    
    try:
        momentum_pct = ticker_data.get("close_30d_delta_pct")
        if pd.isna(momentum_pct):
            momentum_pct = None

        outlook_signal, momentum_context = _get_signal_and_context(
            ticker_data["weighted_score"],
            momentum_pct
        )

        # --- THIS IS THE FIX ---
        # Added the emoji map and selected the correct emoji based on the signal.
        emoji_map = {
            "Strongly Bullish": "🚀",
            "Moderately Bullish": "⬆️",
            "Neutral / Mixed": "⚖️",
            "Moderately Bearish": "⬇️",
            "Strongly Bearish": "🧨"
        }
        signal_emoji = emoji_map.get(outlook_signal, "⚖️")

        raw_macro_trend = (ticker_data.get("macro_trend") or "").strip()
        raw_anti_thesis = (ticker_data.get("anti_thesis") or "").strip()
        macro_trend = raw_macro_trend or "No macro trend provided."
        anti_thesis = raw_anti_thesis or "No macro anti-thesis provided."

        prompt = _PROMPT_TEMPLATE.format(
            ticker=ticker,
            company_name=ticker_data["company_name"],
            signal_emoji=signal_emoji,
            outlook_signal=outlook_signal,
            momentum_context=momentum_context,
            aggregated_text=ticker_data["aggregated_text"],
            macro_trend=macro_trend,
            anti_thesis=anti_thesis,
            example_output=_EXAMPLE_OUTPUT
        )

        recommendation_text = vertex_ai.generate(prompt)

        if not recommendation_text:
            logging.error(f"[{ticker}] LLM returned no text. Aborting.")
            return None

        options_recommendations: list | None = None
        options_parse_error: str | None = None

        json_block_match = re.search(r"```json\s*(\[.*?\])\s*```", recommendation_text, re.DOTALL)
        raw_json = None
        if json_block_match:
            raw_json = json_block_match.group(1)
        else:
            alt_match = re.search(r"(\[\s*\{.*?\}\s*\])", recommendation_text, re.DOTALL)
            if alt_match:
                raw_json = alt_match.group(1)

        if raw_json:
            try:
                parsed_options = json.loads(raw_json)
                if isinstance(parsed_options, dict):
                    parsed_options = [parsed_options]
                elif not isinstance(parsed_options, list):
                    options_parse_error = "Options JSON must be a list or object."
                    parsed_options = []
                options_recommendations = parsed_options
            except json.JSONDecodeError as exc:
                options_parse_error = f"Failed to parse options JSON: {exc}"
        else:
            options_parse_error = "Options JSON block not found in model output."

        metadata = {
            "ticker": ticker,
            "run_date": today_str,
            "outlook_signal": outlook_signal,
            "momentum_context": momentum_context,
            "weighted_score": ticker_data["weighted_score"],
            "macro_trend": raw_macro_trend,
            "anti_thesis": raw_anti_thesis,
            "macro_generated_at": ticker_data.get("macro_generated_at"),
            "recommendation_md_path": f"gs://{config.GCS_BUCKET_NAME}/{md_blob_path}",
            "narrative_brief_markdown": recommendation_text,
            "options_recommendations": options_recommendations or [],
        }

        if options_parse_error:
            metadata["options_parse_error"] = options_parse_error

        _delete_old_recommendation_files(ticker)

        gcs.write_text(config.GCS_BUCKET_NAME, md_blob_path, recommendation_text, "text/markdown")
        gcs.write_text(config.GCS_BUCKET_NAME, json_blob_path, json.dumps(metadata, indent=2), "application/json")
        
        logging.info(f"[{ticker}] Successfully generated and wrote recommendation files to {md_blob_path} and {json_blob_path}")
        return md_blob_path
        
    except Exception as e:
        logging.error(f"[{ticker}] Unhandled exception in processing: {e}", exc_info=True)
        return None

def run_pipeline():
    logging.info("--- Starting Advanced Recommendation Generation Pipeline ---")
    
    work_list = _get_daily_work_list()
    if not work_list:
        return
    
    processed_count = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS_RECOMMENDER) as executor:
        future_to_ticker = {
            executor.submit(_process_ticker, item): item['ticker']
            for item in work_list
        }
        for future in as_completed(future_to_ticker):
            if future.result():
                processed_count += 1
                
    logging.info(f"--- Recommendation Pipeline Finished. Processed {processed_count} of {len(work_list)} tickers. ---")