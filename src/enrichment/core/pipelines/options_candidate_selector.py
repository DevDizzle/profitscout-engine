# src/enrichment/core/pipelines/options_candidate_selector.py
import logging
from google.cloud import bigquery
from .. import config

PROJECT = config.PROJECT_ID
DATASET = config.BIGQUERY_DATASET

CHAIN_TABLE  = f"{PROJECT}.{DATASET}.options_chain"
CAND_TABLE   = f"{PROJECT}.{DATASET}.options_candidates"
PRICE_TABLE  = f"{PROJECT}.{DATASET}.price_data"
SCORES_TABLE = config.SCORES_TABLE_ID

def _create_candidates_table(bq: bigquery.Client):
    """
    Selects option contracts using 'Absolute Conviction' thresholds.
    
    The Logic:
    1. FILTER: Ignore all stocks with a Weighted Score between 0.45 and 0.55 (Neutral).
    
    2. TIER 1: "Rip Hunters" (Strong Conviction)
       - Score >= 0.70 (Strongly Bullish) OR <= 0.30 (Strongly Bearish) OR Breaking News.
       - Rules: 10 DTE min, 25% Spread max, 15% OTM max.
       
    3. TIER 2: "Swing Traders" (Moderate Conviction)
       - Score 0.55-0.70 (Mod Bullish) OR 0.30-0.45 (Mod Bearish).
       - Rules: 14 DTE min, 20% Spread max, High Liquidity (Strict safety).
    """
    
    logging.info(f"Dropping {CAND_TABLE} to ensure clean schema creation...")
    try:
        bq.query(f"DROP TABLE IF EXISTS `{CAND_TABLE}`").result()
    except Exception as e:
        logging.warning(f"Error dropping table (proceeding anyway): {e}")

    logging.info(f"Creating {CAND_TABLE} with ABSOLUTE SCORE & TIGHTENED Logic...")
    
    q = f"""
    CREATE OR REPLACE TABLE `{CAND_TABLE}`
    PARTITION BY DATE(selection_run_ts)
    CLUSTER BY ticker, option_type
    AS
    WITH latest_scores AS (
      -- Get the latest absolute weighted_scores
      SELECT ticker, weighted_score, news_score
      FROM `{SCORES_TABLE}`
      -- OPTIMIZATION: Filter out Neutrals immediately to reduce processing
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
    base AS (
      SELECT
        c.*,
        CASE
          WHEN c.bid > 0 AND c.ask > 0 THEN (c.bid + c.ask) / 2.0
          WHEN c.last_price > 0        THEN c.last_price
          ELSE NULL
        END AS mid_px,
        COALESCE(c.underlying_price, px.adj_close) AS uprice,
        LOWER(c.option_type) AS option_type_lc
      FROM chain_scoped c
      LEFT JOIN px_latest px USING (ticker)
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
        END AS is_uoa
      FROM enriched e
      JOIN latest_scores s ON e.ticker = s.ticker
      WHERE 
        e.spread_pct IS NOT NULL 
        
        AND (
            -- ==================================================
            -- TIER 1: RIP HUNTERS (Strong Conviction)
            -- ==================================================
            (
                -- Condition: Absolute Score > 0.70 (Bull) or < 0.30 (Bear) or News Catalyst
                (s.weighted_score >= 0.70 OR s.weighted_score <= 0.30 OR s.news_score >= 0.90)
                AND (
                    -- Constraints: 10 DTE min, 25% Spread max (Tightened)
                    e.spread_pct <= 0.25
                    AND e.vol_nz >= 50
                    AND e.dte BETWEEN 10 AND 60
                    -- Moneyness: 15% OTM max (Tightened)
                    AND (
                        (e.option_type_lc = 'call' AND e.mny_call BETWEEN 1.00 AND 1.15) OR
                        (e.option_type_lc = 'put'  AND e.mny_put  BETWEEN 1.00 AND 1.15)
                    )
                )
            )
            OR
            -- ==================================================
            -- TIER 2: SWING TRADERS (Moderate Conviction)
            -- ==================================================
            (
                -- Condition: Score is Moderate (0.55-0.70 or 0.30-0.45)
                -- (Neutrals already removed by WHERE clause)
                (
                    e.spread_pct <= 0.20
                    AND e.vol_nz >= 500
                    AND e.oi_nz >= 200
                    AND e.dte BETWEEN 14 AND 45
                    -- Moneyness: 15% OTM max
                    AND (
                        (e.option_type_lc = 'call' AND e.mny_call BETWEEN 1.00 AND 1.15) OR
                        (e.option_type_lc = 'put'  AND e.mny_put  BETWEEN 1.00 AND 1.15)
                    )
                )
            )
        )
    ),
    ranked AS (
      SELECT
        f.*,
        -- Score favors Volume and Tight Spreads
        (
           (SAFE_DIVIDE(vol_nz, 1000) * 0.4) + 
           ((1 - spread_pct) * 0.4) + 
           (CASE WHEN is_uoa THEN 0.2 ELSE 0 END)
        ) as options_score,
        
        -- Pick the #1 best contract per Ticker/Side
        ROW_NUMBER() OVER (
          PARTITION BY ticker, option_type_lc
          ORDER BY vol_nz DESC, oi_nz DESC
        ) AS rn
      FROM filtered f
    )
    SELECT
      CURRENT_TIMESTAMP() AS selection_run_ts,
      ticker, signal,
      contract_symbol, option_type, expiration_date, strike,
      last_price, bid, ask, volume, open_interest, implied_volatility,
      delta, theta, vega, gamma, underlying_price, fetch_date,
      options_score, rn, is_uoa
    FROM ranked
    """

    job = bq.query(q)
    job.result()
    logging.info("Created %s with %s rows using ABSOLUTE SCORE Logic.", CAND_TABLE, job.num_dml_affected_rows or "unknown")


def run_pipeline(bq_client: bigquery.Client | None = None):
    logging.info("--- Starting Options Candidate Selector (Absolute Conviction) ---")
    bq = bq_client or bigquery.Client(project=PROJECT)
    _create_candidates_table(bq)
    logging.info("--- Options Candidate Selector Finished ---")