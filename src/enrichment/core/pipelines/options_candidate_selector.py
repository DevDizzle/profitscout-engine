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
    Selects option contracts using a 'Stock-First' Conviction strategy.
    
    Tier 1: "Rip Hunters" (High Conviction)
    - If the stock is in the Top/Bottom 20% OR has breaking news (score > 0.9):
    - We loosen filters to capture explosive moves (Gamma/Delta focus).
    - Allow: 3 DTE, 40% Spreads, Lower Liquidity, 25% OTM.
    
    Tier 2: "Swing Traders" (Standard)
    - If the stock is normal/neutral:
    - We enforce strict structure for safety (Theta/Vega focus).
    - Require: 14 DTE, 20% Spreads, High Liquidity, 15% OTM.
    """
    
    logging.info(f"Dropping {CAND_TABLE} to ensure clean schema creation...")
    try:
        bq.query(f"DROP TABLE IF EXISTS `{CAND_TABLE}`").result()
    except Exception as e:
        logging.warning(f"Error dropping table (proceeding anyway): {e}")

    logging.info(f"Creating {CAND_TABLE} with Tiered 'Rip Hunter' Logic...")
    
    q = f"""
    CREATE OR REPLACE TABLE `{CAND_TABLE}`
    PARTITION BY DATE(selection_run_ts)
    CLUSTER BY ticker, option_type
    AS
    WITH latest_scores AS (
      -- Get the latest conviction scores for every ticker
      SELECT ticker, score_percentile, news_score
      FROM `{SCORES_TABLE}`
    ),
    latest_chain_per_ticker AS (
      -- Ensure we only look at the most recent snapshot for each ticker
      SELECT ticker, MAX(fetch_date) AS fetch_date
      FROM `{CHAIN_TABLE}`
      GROUP BY ticker
    ),
    px_latest AS (
      -- Get the latest stock price to calculate accurate moneyness if needed
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
      -- Join the chain data with the 'latest date' filter
      SELECT t.*
      FROM `{CHAIN_TABLE}` t
      JOIN latest_chain_per_ticker l USING (ticker, fetch_date)
    ),
    base AS (
      SELECT
        c.*,
        -- Calculate Mid Price for spread checks
        CASE
          WHEN c.bid > 0 AND c.ask > 0 THEN (c.bid + c.ask) / 2.0
          WHEN c.last_price > 0        THEN c.last_price
          ELSE NULL
        END AS mid_px,
        -- Use Underlying Price from chain, fallback to Price Table if missing
        COALESCE(c.underlying_price, px.adj_close) AS uprice,
        LOWER(c.option_type) AS option_type_lc
      FROM chain_scoped c
      LEFT JOIN px_latest px USING (ticker)
    ),
    enriched AS (
      SELECT
        b.*,
        -- Calculate Spread %: (Ask - Bid) / Mid
        SAFE_DIVIDE(b.ask - b.bid, NULLIF(b.mid_px, 0)) AS spread_pct,
        COALESCE(b.open_interest, 0) AS oi_nz,
        COALESCE(b.volume, 0)       AS vol_nz,
        -- Calculate Moneyness:
        -- Call OTM = Strike / Price (> 1.0)
        SAFE_DIVIDE(b.strike, NULLIF(b.uprice, 0)) AS mny_call,
        -- Put OTM = Price / Strike (> 1.0)
        SAFE_DIVIDE(b.uprice, NULLIF(b.strike, 0)) AS mny_put
      FROM base b
    ),
    filtered AS (
      SELECT
        e.*,
        -- Assign directional signal
        CASE
          WHEN e.option_type_lc = 'call' THEN 'BUY'
          ELSE 'SELL'
        END AS signal,
        -- Flag Unusual Options Activity (Vol > OI is a strong momentum signal)
        CASE 
          WHEN e.vol_nz > e.oi_nz AND e.vol_nz > 500 THEN TRUE 
          ELSE FALSE 
        END AS is_uoa
      FROM enriched e
      LEFT JOIN latest_scores s ON e.ticker = s.ticker
      WHERE 
        -- Base Safety Check
        e.spread_pct IS NOT NULL 
        
        AND (
            -- ==================================================
            -- TIER 1: RIP HUNTERS (High Conviction or Big News)
            -- ==================================================
            (
                -- Condition: Top/Bottom 20% OR Breaking News > 0.90
                (s.score_percentile >= 0.80 OR s.score_percentile <= 0.20 OR s.news_score >= 0.90)
                AND (
                    -- Looser Spread: Up to 40% allowed
                    e.spread_pct <= 0.40
                    -- Lower Volume: Catch moves early (50+ contracts)
                    AND e.vol_nz >= 50
                    -- Aggressive DTE: 3 days to 60 days (Gamma focus)
                    AND e.dte BETWEEN 3 AND 60
                    -- Wider Moneyness: Up to 25% OTM (Lotto tickets allowed)
                    AND (
                        (e.option_type_lc = 'call' AND e.mny_call BETWEEN 1.00 AND 1.25) OR
                        (e.option_type_lc = 'put'  AND e.mny_put  BETWEEN 1.00 AND 1.25)
                    )
                )
            )
            OR
            -- ==================================================
            -- TIER 2: SWING TRADERS (Standard Safety)
            -- ==================================================
            (
                -- Strict Spread: Must be tight (< 20%)
                e.spread_pct <= 0.20
                -- High Liquidity: Validated volume only
                AND e.vol_nz >= 500
                AND e.oi_nz >= 200
                -- Safer DTE: 14 to 45 days (Theta protection)
                AND e.dte BETWEEN 14 AND 45
                -- Conservative Moneyness: Up to 15% OTM
                AND (
                    (e.option_type_lc = 'call' AND e.mny_call BETWEEN 1.00 AND 1.15) OR
                    (e.option_type_lc = 'put'  AND e.mny_put  BETWEEN 1.00 AND 1.15)
                )
            )
        )
    ),
    ranked AS (
      SELECT
        f.*,
        -- SCORING:
        -- Simple robust score. We don't need the complex logic here because
        -- Options Analyzer will do the heavy lifting on "Quality".
        (
           (SAFE_DIVIDE(vol_nz, 1000) * 0.4) + 
           ((1 - spread_pct) * 0.4) + 
           (CASE WHEN is_uoa THEN 0.2 ELSE 0 END)
        ) as options_score,
        
        -- Pick the #1 best contract per Ticker/Side based on volume
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
    logging.info("Created %s with %s rows using 'Rip Hunter' Logic.", CAND_TABLE, job.num_dml_affected_rows or "unknown")


def run_pipeline(bq_client: bigquery.Client | None = None):
    logging.info("--- Starting Options Candidate Selector (Conviction-First Strategy) ---")
    bq = bq_client or bigquery.Client(project=PROJECT)
    _create_candidates_table(bq)
    logging.info("--- Options Candidate Selector Finished ---")
