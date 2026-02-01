# serving/core/pipelines/recommendations_generator.py
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import pandas as pd
from google.cloud import bigquery

from .. import config, gcs


def _get_signal_and_context(
    score: float, momentum_pct: float | None
) -> tuple[str, str]:
    """
    Determines the 5-tier outlook signal based on the ABSOLUTE WEIGHTED SCORE.
    """
    if score >= 0.70:
        outlook = "Strongly Bullish"
    elif 0.55 <= score < 0.70:
        outlook = "Moderately Bullish"
    elif 0.45 <= score < 0.55:
        outlook = "Neutral / Mixed"
    elif 0.30 <= score < 0.45:
        outlook = "Moderately Bearish"
    else:
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

    if outlook == "Neutral / Mixed":
        if score > 0.50:
            outlook += " with a bullish tilt"
        elif score < 0.50:
            outlook += " with a bearish tilt"
        else:
            outlook += " (lacks conviction)"

    return outlook, context


def _get_daily_work_list() -> list[dict]:
    """Builds the work list from GCS and enriches from BigQuery."""
    logging.info("Fetching work list from GCS and enriching from BigQuery...")
    tickers = gcs.get_tickers()
    if not tickers:
        logging.critical("Ticker list from GCS is empty. No work to do.")
        return []

    client = bigquery.Client(project=config.SOURCE_PROJECT_ID)

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
        # Only process if we have the aggregated text analysis
        df.dropna(
            subset=["company_name", "weighted_score", "aggregated_text"], inplace=True
        )
        if df.empty:
            logging.warning("No tickers with sufficient data found after enriching.")
            return []
        logging.info(f"Successfully created work list for {len(df)} tickers.")
        return df.to_dict("records")
    except Exception as e:
        logging.critical(f"Failed to build work list: {e}", exc_info=True)
        return []


def _delete_old_recommendation_files(ticker: str):
    """Deletes old recommendation files."""
    prefix = f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation_"
    blobs_to_delete = gcs.list_blobs(config.GCS_BUCKET_NAME, prefix)
    for blob_name in blobs_to_delete:
        try:
            gcs.delete_blob(config.GCS_BUCKET_NAME, blob_name)
        except Exception as e:
            logging.error(f"[{ticker}] Failed to delete old file {blob_name}: {e}")


def _process_ticker(ticker_data: dict):
    """Generates the recommendation metadata (JSON only)."""
    ticker = ticker_data["ticker"]
    today_str = date.today().strftime("%Y-%m-%d")

    base_blob_path = (
        f"{config.RECOMMENDATION_PREFIX}{ticker}_recommendation_{today_str}"
    )
    json_blob_path = f"{base_blob_path}.json"

    try:
        momentum_pct = ticker_data.get("close_30d_delta_pct")
        if pd.isna(momentum_pct):
            momentum_pct = None

        score = ticker_data.get("weighted_score")
        if pd.isna(score):
            score = 0.5

        outlook_signal, momentum_context = _get_signal_and_context(score, momentum_pct)

        metadata = {
            "ticker": ticker,
            "run_date": today_str,
            "outlook_signal": outlook_signal,
            "momentum_context": momentum_context,
            "weighted_score": ticker_data["weighted_score"],
            "score_percentile": ticker_data.get("score_percentile", 0.5),
        }

        _delete_old_recommendation_files(ticker)
        gcs.write_text(
            config.GCS_BUCKET_NAME,
            json_blob_path,
            json.dumps(metadata, indent=2),
            "application/json",
        )

        return json_blob_path

    except Exception as e:
        logging.error(f"[{ticker}] Processing failed: {e}", exc_info=True)
        return None


def run_pipeline():
    logging.info("--- Starting Recommendation Metadata Pipeline (No LLM) ---")

    work_list = _get_daily_work_list()
    if not work_list:
        return

    processed_count = 0
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS_RECOMMENDER) as executor:
        future_to_ticker = {
            executor.submit(_process_ticker, item): item["ticker"] for item in work_list
        }
        for future in as_completed(future_to_ticker):
            if future.result():
                processed_count += 1

    logging.info(
        f"--- Recommendation Metadata Pipeline Finished. Processed {processed_count}/{len(work_list)} tickers. ---"
    )
