# src/enrichment/core/pipelines/options_candidate_selector.py
import logging

from google.cloud import bigquery

from .. import config

PROJECT = config.PROJECT_ID
DATASET = config.BIGQUERY_DATASET

CHAIN_TABLE = f"{PROJECT}.{DATASET}.options_chain"
CAND_TABLE = f"{PROJECT}.{DATASET}.options_candidates"
PRICE_TABLE = f"{PROJECT}.{DATASET}.price_data"
SCORES_TABLE = config.SCORES_TABLE_ID


def _create_candidates_table(bq: bigquery.Client):
    """
    Selects option contracts using a Pure Fundamental Approach:
    1. FUNDAMENTAL CONVICTION (Tier 1): High LLM Score -> Safe Options.
    """

    logging.info(f"Dropping {CAND_TABLE} to ensure clean schema creation...")
    try:
        bq.query(f"DROP TABLE IF EXISTS `{CAND_TABLE}`").result()
    except Exception as e:
        logging.warning(f"Error dropping table (proceeding anyway): {e}")

    logging.info(f"Creating {CAND_TABLE} with TIER 1 (Standard/Conviction) Logic...")

    q = f"""
    CREATE OR REPLACE TABLE `{CAND_TABLE}`
    PARTITION BY DATE(selection_run_ts)
    CLUSTER BY ticker, option_type
    AS
    WITH latest_scores AS (
      SELECT ticker, weighted_score, news_score
      FROM `{SCORES_TABLE}`
      -- Filter out neutrals for Tier 1
      WHERE weighted_score > 0.55 OR weighted_score < 0.45 OR news_score >= 0.90
    ),
    latest_chain_per_ticker AS (
      SELECT ticker, MAX(fetch_date) AS fetch_date
      FROM `{CHAIN_TABLE}`
      GROUP BY ticker
    ),
    px_latest AS (
      SELECT ticker, adj_close
      FROM (
        SELECT
          p.ticker,
          p.adj_close,
          ROW_NUMBER() OVER (PARTITION BY p.ticker ORDER BY p.date DESC) AS rn
        FROM `{PRICE_TABLE}` p
      )
      WHERE rn = 1
    ),
    chain_scoped AS (
      SELECT t.*
      FROM `{CHAIN_TABLE}` t
      JOIN latest_chain_per_ticker l USING (ticker, fetch_date)
    ),
    sentiment AS (
      SELECT
        ticker,
        SAFE_DIVIDE(SUM(CASE WHEN LOWER(option_type)='put' THEN volume ELSE 0 END),
                    NULLIF(SUM(CASE WHEN LOWER(option_type)='call' THEN volume ELSE 0 END), 0)) as pc_ratio
      FROM chain_scoped
      GROUP BY ticker
    ),
    base AS (
      SELECT
        c.*,
        CASE
          WHEN c.bid > 0 AND c.ask > 0 THEN (c.bid + c.ask) / 2.0
          WHEN c.last_price > 0        THEN c.last_price
          ELSE NULL
        END AS mid_px,
        COALESCE(c.underlying_price, px.adj_close) AS uprice,
        LOWER(c.option_type) AS option_type_lc,
        sen.pc_ratio
      FROM chain_scoped c
      LEFT JOIN px_latest px USING (ticker)
      LEFT JOIN sentiment sen USING (ticker)
    ),
    enriched AS (
      SELECT
        b.*,
        SAFE_DIVIDE(b.ask - b.bid, NULLIF(b.mid_px, 0)) AS spread_pct,
        COALESCE(b.open_interest, 0) AS oi_nz,
        COALESCE(b.volume, 0)       AS vol_nz,
        SAFE_DIVIDE(b.strike, NULLIF(b.uprice, 0)) AS mny_call,
        SAFE_DIVIDE(b.uprice, NULLIF(b.strike, 0)) AS mny_put
      FROM base b
    ),
    filtered AS (
      SELECT
        e.*,
        CASE
          WHEN e.option_type_lc = 'call' THEN 'BUY'
          ELSE 'SELL'
        END AS signal,
        CASE
          WHEN e.vol_nz > e.oi_nz AND e.vol_nz > 500 THEN TRUE
          ELSE FALSE
        END AS is_uoa,

        -- Default to False for consistency
        FALSE AS is_ml_pick

      FROM enriched e
      LEFT JOIN latest_scores s ON e.ticker = s.ticker
      WHERE
        e.spread_pct IS NOT NULL

        -- [SWING TRADER BASELINE STANDARDS]
        AND e.dte BETWEEN 14 AND 60         -- 14-60 Days: Time for trade to materialize
        AND e.spread_pct <= 0.20            -- Max 20% Spread: No liquidity traps
        AND (e.vol_nz >= 250 OR e.oi_nz >= 500) -- High Liquidity Only

        AND (
            -- ==================================================
            -- TIER 1: RIP HUNTERS (Strong Fundamental Conviction)
            -- ==================================================
            s.weighted_score IS NOT NULL
            AND (s.weighted_score >= 0.70 OR s.weighted_score <= 0.30 OR s.news_score >= 0.90)
            AND (
                (e.option_type_lc = 'call' AND e.mny_call BETWEEN 1.00 AND 1.15) OR
                (e.option_type_lc = 'put'  AND e.mny_put  BETWEEN 1.00 AND 1.15)
            )
        )
    ),
    scored AS (
      SELECT
        f.*,
        -- Scoring: Pure Fundamental/Greeks
        (
           (COALESCE(gamma, 0) * 20.0) +

           (LEAST(SAFE_DIVIDE(vol_nz, 1000), 5.0) * 0.4) +

           (CASE WHEN spread_pct <= 0.25 THEN (1.0 - (spread_pct * 4.0)) ELSE 0 END) +

           (CASE
               WHEN (option_type_lc = 'call' AND mny_call BETWEEN 0.95 AND 1.05) THEN 0.5
               WHEN (option_type_lc = 'put' AND mny_put BETWEEN 0.95 AND 1.05) THEN 0.5
               ELSE 0
            END) +

           (CASE WHEN is_uoa THEN 0.2 ELSE 0 END)
        ) as options_score
      FROM filtered f
    ),
    ranked AS (
      SELECT
        s.*,
        -- Pick the #1 best contract per Ticker/Side
        ROW_NUMBER() OVER (
          PARTITION BY ticker, option_type_lc
          ORDER BY options_score DESC
        ) AS rn
      FROM scored s
    )
    SELECT
      CURRENT_TIMESTAMP() AS selection_run_ts,
      ticker, signal,
      contract_symbol, option_type, expiration_date, strike,
      last_price, bid, ask, volume, open_interest, implied_volatility,
      delta, theta, vega, gamma, underlying_price, fetch_date,
      options_score, rn, is_uoa, is_ml_pick, CAST('STANDARD' AS STRING) as strategy, pc_ratio
    FROM ranked
    """

    job = bq.query(q)
    job.result()
    logging.info("Created %s with Tier 1 (Fundamental Conviction) Logic.", CAND_TABLE)


def run_pipeline(bq_client: bigquery.Client | None = None):
    logging.info("--- Starting Options Candidate Selector (Strict) ---")
    bq = bq_client or bigquery.Client(project=PROJECT)
    _create_candidates_table(bq)
    logging.info("--- Options Candidate Selector Finished ---")
