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
DAILY_PRED_TABLE = f"{PROJECT}.{DATASET}.daily_predictions" # [NEW]

def _create_candidates_table(bq: bigquery.Client):
    """
    Selects option contracts using a Hybrid Approach:
    1. FUNDAMENTAL CONVICTION (Tier 1/2): High LLM Score -> Safe Options.
    2. ML SNIPER (Tier 3): High ML Probability -> Aggressive Options.
    """
    
    logging.info(f"Dropping {CAND_TABLE} to ensure clean schema creation...")
    try:
        bq.query(f"DROP TABLE IF EXISTS `{CAND_TABLE}`").result()
    except Exception as e:
        logging.warning(f"Error dropping table (proceeding anyway): {e}")

    logging.info(f"Creating {CAND_TABLE} with TIER 3 (ML SNIPER) Logic...")
    
    q = f"""
    CREATE OR REPLACE TABLE `{CAND_TABLE}`
    PARTITION BY DATE(selection_run_ts)
    CLUSTER BY ticker, option_type
    AS
    WITH latest_scores AS (
      SELECT ticker, weighted_score, news_score
      FROM `{SCORES_TABLE}`
      -- Filter out neutrals for Tier 1/2, but we keep this loose for joining
      WHERE weighted_score > 0.55 OR weighted_score < 0.45 OR news_score >= 0.90
    ),
    ml_picks AS (
      -- [NEW] Fetch Top 10 ML Predictions for today (Global Rank)
      -- Normalize contract_type: LONG -> CALL, SHORT -> PUT to match Option Types
      SELECT 
        ticker, 
        CASE 
            WHEN UPPER(contract_type) IN ('LONG', 'CALL') THEN 'CALL'
            WHEN UPPER(contract_type) IN ('SHORT', 'PUT') THEN 'PUT'
            ELSE UPPER(contract_type)
        END AS ml_direction, 
        prob
      FROM `{DAILY_PRED_TABLE}`
      WHERE date = (SELECT MAX(date) FROM `{DAILY_PRED_TABLE}`)
      QUALIFY ROW_NUMBER() OVER (ORDER BY prob DESC) <= 10
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
        END AS is_uoa,
        
        -- [UPDATED] Flag ONLY if ML direction matches AND aligns with Fundamentals (Confluence)
        -- Prevents "Strong Buy" stocks getting "Put" recommendations.
        CASE 
            WHEN m.ticker IS NOT NULL 
                 AND UPPER(e.option_type) = m.ml_direction 
                 AND (
                    (m.ml_direction = 'CALL' AND COALESCE(s.weighted_score, 0.5) >= 0.45) OR 
                    (m.ml_direction = 'PUT'  AND COALESCE(s.weighted_score, 0.5) <= 0.55)
                 )
            THEN TRUE 
            ELSE FALSE 
        END AS is_ml_pick
        
      FROM enriched e
      LEFT JOIN latest_scores s ON e.ticker = s.ticker
      LEFT JOIN ml_picks m ON e.ticker = m.ticker
      WHERE 
        e.spread_pct IS NOT NULL 
        
        -- [SWING TRADER BASELINE STANDARDS]
        AND e.dte BETWEEN 14 AND 60         -- 14-60 Days: Time for trade to materialize
        AND e.spread_pct <= 0.20            -- Max 20% Spread: No liquidity traps
        AND (e.vol_nz >= 250 OR e.oi_nz >= 500) -- High Liquidity Only
        
        AND (
            -- ==================================================
            -- TIER 3: SWING SNIPER (ML + Confluence)
            -- ==================================================
            (
                m.ticker IS NOT NULL
                AND UPPER(e.option_type) = m.ml_direction
                -- Alignment Check (Must match logic in is_ml_pick above)
                AND (
                    (m.ml_direction = 'CALL' AND COALESCE(s.weighted_score, 0.5) >= 0.45) OR 
                    (m.ml_direction = 'PUT'  AND COALESCE(s.weighted_score, 0.5) <= 0.55)
                )
                AND (
                    (e.option_type_lc = 'call' AND e.mny_call BETWEEN 0.95 AND 1.10) OR -- Slightly OTM/ATM
                    (e.option_type_lc = 'put'  AND e.mny_put  BETWEEN 0.95 AND 1.10)
                )
            )
            OR
            -- ==================================================
            -- TIER 1: RIP HUNTERS (Strong Fundamental Conviction)
            -- ==================================================
            (
                s.weighted_score IS NOT NULL
                AND (s.weighted_score >= 0.70 OR s.weighted_score <= 0.30 OR s.news_score >= 0.90)
                AND (
                    (e.option_type_lc = 'call' AND e.mny_call BETWEEN 0.90 AND 1.15) OR
                    (e.option_type_lc = 'put'  AND e.mny_put  BETWEEN 0.90 AND 1.15)
                )
            )
        )
    ),
    scored AS (
      SELECT
        f.*,
        -- Scoring: ML Picks get a massive boost to ensure they rank #1
        -- Tightened Logic:
        -- 1. Volume Capped at 5k (approx 2 pts max)
        -- 2. Spread Penalty: Linear decay. 0% spread = +1.0, 25% spread = 0.0
        -- 3. Gamma Bonus: +0.5 for NTM (0.95-1.05)
        (
           (CASE WHEN is_ml_pick THEN 100 ELSE 0 END) +
           
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
          ORDER BY (CASE WHEN is_ml_pick THEN 1 ELSE 0 END) DESC, options_score DESC
        ) AS rn
      FROM scored s
    )
    SELECT
      CURRENT_TIMESTAMP() AS selection_run_ts,
      ticker, signal,
      contract_symbol, option_type, expiration_date, strike,
      last_price, bid, ask, volume, open_interest, implied_volatility,
      delta, theta, vega, gamma, underlying_price, fetch_date,
      options_score, rn, is_uoa, is_ml_pick
    FROM ranked
    """

    job = bq.query(q)
    job.result()
    logging.info("Created %s with Tier 3 (ML Sniper) Logic.", CAND_TABLE)


def run_pipeline(bq_client: bigquery.Client | None = None):
    logging.info("--- Starting Options Candidate Selector (With ML Sniper) ---")
    bq = bq_client or bigquery.Client(project=PROJECT)
    _create_candidates_table(bq)
    logging.info("--- Options Candidate Selector Finished ---")