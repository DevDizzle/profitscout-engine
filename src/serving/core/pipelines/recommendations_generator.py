# serving/core/pipelines/recommendations_generator.py
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from .. import config, gcs
from ..clients import vertex_ai
from datetime import date, datetime
import re
import json

# --- ENHANCED PROMPT: For Directional Premium Buyers, No Example ---
_PROMPT_TEMPLATE = r"""
You are a trading analyst writing a brief for a short-term, directional options trader who BUYS premium (calls/puts).
The trader's goal is to capture a significant price move (2-5% or more) over the next 1-5 trading weeks. Your analysis must focus on identifying potential breakout catalysts and confirming technical momentum.

Use ONLY the facts from the `Aggregated Analysis Text`.

### Output Format (Strict)

# {company_name} ({ticker}) {signal_emoji}

**Outlook:** {outlook_signal} {momentum_context}

**Business snapshot:**
A tight, one-sentence summary of what the company does.

## Directional Thesis (2-4 bullets)
- Explain WHY this stock might make a significant move soon.
- Each bullet must combine a theme (e.g., "Technical Breakout Confirmation," "News Catalyst," "Fundamental Shift") with specific data from the input and its direct impact on breakout potential.
- Focus on signals of building momentum: price breaking key MAs, strong RSI/MACD readings, high-impact news, or earnings surprises.

## Breakout Catalysts (max 3)
- List the most likely events that could trigger a sharp, volatile move in the near term.
- Examples: Upcoming earnings reports, product launches, major economic data relevant to the sector, or guidance updates.

## Key Price Levels
- **Support:** <Critical support level(s) below which the bullish thesis weakens.>
- **Resistance:** <Critical resistance level(s) that must be broken for a bullish breakout.>
- **Invalidation Point:** <A single price level or condition that clearly invalidates the primary directional thesis.>

## Risks to the Trade
- The 1-2 most significant factors that could prevent the breakout or cause a reversal (e.g., "Failure at 50-day SMA," "Wider market weakness," "Disappointing guidance").

## The Bottom Line
One concise paragraph synthesizing why the setup favors a directional breakout. State the primary thesis clearly and what would invalidate it. This should directly address the goal of capturing a sharp, near-term move.

### Critical Rules
1.  **Focus on Breakouts**: Your entire analysis must be through the lens of a premium buyer looking for a volatile move, not a range-bound stock.
2.  **Quantify Everything**: Use specific numbers, levels, and indicators from the source text.
3.  **No Examples**: Generate the output based solely on these instructions and the provided data. Do not replicate any previous examples.
4.  **No Advice**: Use an informational, analytical tone (e.g., "suggests," "indicates," "could").
5.  **No Options Jargon**: The output is about the underlying stock's direction and volatility potential.

### Input Data
- **Outlook Signal**: {outlook_signal}
- **Momentum Context**: {momentum_context}
- **Aggregated Analysis Text**:
{aggregated_text}

### Signal → Emoji map (internal)
- Strongly Bullish: 🚀
- Moderately Bullish: ⬆️
- Neutral / Mixed: ⚖️
- Moderately Bearish: ⬇️
- Strongly Bearish: 🧨
"""


def _get_signal_and_context(score: float, momentum_pct: float | None) -> tuple[str, str]:
    """
    Determines the 5-tier outlook signal based on the ABSOLUTE WEIGHTED SCORE.
    This ensures the label matches the semantic analysis (e.g., Bearish Technicals).
    """
    # Absolute Thresholds based on 0.0-1.0 scale (0.5 is Neutral)
    if score >= 0.70:
        outlook = "Strongly Bullish"
    elif 0.55 <= score < 0.70:
        outlook = "Moderately Bullish"
    elif 0.45 <= score < 0.55:
        outlook = "Neutral / Mixed"
    elif 0.30 <= score < 0.45:
        outlook = "Moderately Bearish"
    else:  # score < 0.30
        outlook = "Strongly Bearish"

    context = ""
    if momentum_pct is not None:
        is_bullish_outlook = "Bullish" in outlook
        is_bearish_outlook = "Bearish" in outlook

        if is_bullish_outlook and momentum_pct > 0:
            context = "with confirming positive momentum."
        elif is_bullish_outlook and momentum_pct < 0:
            context = "but facing a short-term pullback."
        elif is_bearish_outlook and momentum_pct < 0:
            context = "with confirming negative momentum."
        elif is_bearish_outlook and momentum_pct > 0:
            context = "but facing a short-term counter-rally."
    
    # Add a directional "tilt" to the neutral signal to make it more useful
    if outlook == "Neutral / Mixed":
        if score > 0.50:
            outlook += " with a bullish tilt"
        elif score < 0.50:
            outlook += " with a bearish tilt"
        else:
            outlook += " (lacks conviction)"

    return outlook, context


def _get_daily_work_list() -> list[dict]:
    """
    Builds the work list from the GCS tickerlist.txt and enriches it
    with the latest available data from BigQuery for each ticker.
    """
    logging.info("Fetching work list from GCS and enriching from BigQuery...")
    tickers = gcs.get_tickers()
    if not tickers:
        logging.critical("Ticker list from GCS is empty. No work to do.")
        return []
        
    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)
    
    # Fetch score_percentile in addition to weighted_score
    query = f"""
        WITH GCS_Tickers AS (
            SELECT ticker FROM UNNEST(@tickers) AS ticker
        ),
        RankedScores AS (
            SELECT
                t1.ticker,
                t2.company_name,
                t1.weighted_score,
                t1.score_percentile,
                t1.aggregated_text,
                ROW_NUMBER() OVER(
                    PARTITION BY t1.ticker
                    ORDER BY t1.run_date DESC, t1.weighted_score DESC
                ) as rn
            FROM `{config.SCORES_TABLE_ID}` AS t1
            JOIN `{config.BUNDLER_STOCK_METADATA_TABLE_ID}` AS t2
              ON t1.ticker = t2.ticker
            WHERE t1.weighted_score IS NOT NULL
              AND t2.company_name IS NOT NULL
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
                    ROW_NUMBER() OVER(
                        PARTITION BY ticker ORDER BY date DESC
                    ) as rn
                FROM `{config.SOURCE_PROJECT_ID}.{config.BIGQUERY_DATASET}.options_analysis_input`
                WHERE close_30d_delta_pct IS NOT NULL
            )
            WHERE rn = 1
        )
        SELECT
            g.ticker,
            s.company_name,
            s.weighted_score,
            s.score_percentile,
            s.aggregated_text,
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
        df.dropna(subset=["company_name", "weighted_score", "aggregated_text"], inplace=True)
        if df.empty:
            logging.warning("No tickers with sufficient data found after enriching from BigQuery.")
            return []
        logging.info(f"Successfully created work list for {len(df)} tickers.")
        return df.to_dict("records")
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
    today_str = date.today().strftime("%Y-%m-%d")
    
    base_blob_path = f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation_{today_str}"
    md_blob_path = f"{base_blob_path}.md"
    json_blob_path = f"{base_blob_path}.json"
    
    try:
        momentum_pct = ticker_data.get("close_30d_delta_pct")
        if pd.isna(momentum_pct):
            momentum_pct = None

        # --- MODIFIED: Use absolute weighted_score for signal definition ---
        score = ticker_data.get("weighted_score")
        if pd.isna(score):
            score = 0.5

        outlook_signal, momentum_context = _get_signal_and_context(
            score,
            momentum_pct,
        )

        emoji_map = {
            "Strongly Bullish": "🚀",
            "Moderately Bullish": "⬆️",
            "Neutral / Mixed": "⚖️",
            "Moderately Bearish": "⬇️",
            "Strongly Bearish": "🧨",
        }
        # Handle cases where outlook has " with a..." or "(lacks...)"
        base_outlook = outlook_signal.split(" with a")[0].split(" (")[0].strip()
        signal_emoji = emoji_map.get(base_outlook, "⚖️")

        prompt = _PROMPT_TEMPLATE.format(
            ticker=ticker,
            company_name=ticker_data["company_name"],
            signal_emoji=signal_emoji,
            outlook_signal=outlook_signal,
            momentum_context=momentum_context,
            aggregated_text=ticker_data["aggregated_text"],
        )
        
        recommendation_text = vertex_ai.generate(prompt)

        if not recommendation_text:
            logging.error(f"[{ticker}] LLM returned no text. Aborting.")
            return None
        
        metadata = {
            "ticker": ticker,
            "run_date": today_str,
            "outlook_signal": outlook_signal,
            "momentum_context": momentum_context,
            "weighted_score": ticker_data["weighted_score"],
            # Store percentile for reference, even if not used for signal
            "score_percentile": ticker_data.get("score_percentile", 0.5), 
            "recommendation_md_path": f"gs://{config.GCS_BUCKET_NAME}/{md_blob_path}",
        }
        
        _delete_old_recommendation_files(ticker)
        
        gcs.write_text(config.GCS_BUCKET_NAME, md_blob_path, recommendation_text, "text/markdown")
        gcs.write_text(config.GCS_BUCKET_NAME, json_blob_path, json.dumps(metadata, indent=2), "application/json")
        
        logging.info(
            f"[{ticker}] Successfully generated and wrote recommendation files to "
            f"{md_blob_path} and {json_blob_path}"
        )
        return md_blob_path
        
    except Exception as e:
        logging.error(f"[{ticker}] Unhandled exception in processing: {e}", exc_info=True)
        return None


def run_pipeline():
    logging.info("--- Starting Directional Trading Brief Pipeline ---")
    
    work_list = _get_daily_work_list()
    if not work_list:
        return
    
    processed_count = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS_RECOMMENDER) as executor:
        future_to_ticker = {
            executor.submit(_process_ticker, item): item["ticker"]
            for item in work_list
        }
        for future in as_completed(future_to_ticker):
            if future.result():
                processed_count += 1
                
    logging.info(
        f"--- Recommendation Pipeline Finished. "
        f"Processed {processed_count} of {len(work_list)} tickers. ---"
    )