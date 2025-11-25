# src/enrichment/core/pipelines/options_candidate_selector.py
import logging
from google.cloud import bigquery
from .. import config

PROJECT = config.PROJECT_ID
DATASET = config.BIGQUERY_DATASET

CHAIN_TABLE  = f"{PROJECT}.{DATASET}.options_chain"
CAND_TABLE   = f"{PROJECT}.{DATASET}.options_candidates"
PRICE_TABLE  = f"{PROJECT}.{DATASET}.price_data"


def _create_candidates_table(bq: bigquery.Client):
    """
    Selects high-quality option contracts specifically for 'Premium Buying' swing trades.
    Focuses on liquidity, reasonable spreads, and 'sweet spot' leverage (ATM to slightly OTM).
    """
    
    # --- STEP 1: Drop the existing table ---
    # Dropping ensures we don't have schema conflicts if we change columns/partitioning.
    logging.info(f"Dropping {CAND_TABLE} to ensure clean schema creation...")
    try:
        bq.query(f"DROP TABLE IF EXISTS `{CAND_TABLE}`").result()
    except Exception as e:
        logging.warning(f"Error dropping table (proceeding anyway): {e}")

    # --- STEP 2: Create the table with Premium Buying Logic ---
    logging.info(f"Creating {CAND_TABLE} with Premium Buyer filters...")
    q = f"""
    CREATE OR REPLACE TABLE `{CAND_TABLE}`
    PARTITION BY DATE(selection_run_ts)
    CLUSTER BY ticker, option_type
    AS
    WITH params AS (
      SELECT
        -- TIME: Avoid <14d (gamma risk/theta burn). Cap at 60d for swing trading.
        14    AS min_dte,
        60    AS max_dte,
        
        -- CALLS: Target ATM (1.00) to 15% OTM (1.15). Good leverage, decent probability.
        1.00  AS min_mny_call,
        1.15  AS max_mny_call,
        
        -- PUTS: Target ATM (1.00) to 15% OTM (1.15). Note: Puts OTM are Strike < Price.
        1.00  AS min_mny_put,
        1.15  AS max_mny_put,
        
        -- LIQUIDITY: Ensure entry/exit is smooth.
        200   AS min_oi,
        50    AS min_vol,
        
        -- COST: Max spread of 20%. Anything higher is too expensive to trade.
        0.20  AS max_spread
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
      CROSS JOIN params p
      WHERE 
        -- 1. DTE Filter
        e.dte BETWEEN p.min_dte AND p.max_dte
        -- 2. Moneyness Filter (Buying OTM/ATM Premium)
        AND (
              (e.option_type_lc = 'call'
               AND e.mny_call BETWEEN p.min_mny_call AND p.max_mny_call)
            OR (e.option_type_lc = 'put'
               AND e.mny_put  BETWEEN p.min_mny_put  AND p.max_mny_put)
        )
        -- 3. Liquidity Filters
        AND e.oi_nz >= p.min_oi
        AND e.vol_nz >= p.min_vol
        -- 4. Spread Filter (Quality Check)
        AND e.spread_pct IS NOT NULL AND e.spread_pct <= p.max_spread
    ),
    ranked AS (
      SELECT
        f.*,
        -- SCORING: Rank the best contracts for the dashboard.
        -- A simple, robust score: 
        -- 40% Liquidity (Volume/OI) + 40% Tight Spread + 20% UOA Bonus
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
    logging.info("Created %s with %s rows.", CAND_TABLE, job.num_dml_affected_rows or "unknown")


def run_pipeline(bq_client: bigquery.Client | None = None):
    logging.info("--- Starting Options Candidate Selector (Premium Buyer Strategy) ---")
    bq = bq_client or bigquery.Client(project=PROJECT)
    _create_candidates_table(bq)
    logging.info("--- Options Candidate Selector Finished ---")