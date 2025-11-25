# enrichment/core/pipelines/options_candidate_selector.py
import logging
from google.cloud import bigquery
from .. import config

PROJECT = config.PROJECT_ID
DATASET = config.BIGQUERY_DATASET

SCORES_TABLE = f"{PROJECT}.{DATASET}.analysis_scores"
CHAIN_TABLE  = f"{PROJECT}.{DATASET}.options_chain"
CAND_TABLE   = f"{PROJECT}.{DATASET}.options_candidates"


def _create_candidates_table(bq: bigquery.Client):
    """
    Options candidate selector with elasticity-aware scoring AND Unusual Options Activity (UOA) detection.
    
    Strategy: DROP and RECREATE the table.
    This avoids BigQuery errors when the partitioning spec changes and ensures a clean slate.
    """
    price_table = f"{PROJECT}.{DATASET}.price_data"

    # --- STEP 1: Drop the existing table ---
    # This allows us to change partitioning/clustering specs without error.
    logging.info(f"Dropping {CAND_TABLE} to ensure clean schema creation...")
    try:
        bq.query(f"DROP TABLE IF EXISTS `{CAND_TABLE}`").result()
        logging.info(f"Table {CAND_TABLE} dropped (if it existed).")
    except Exception as e:
        logging.warning(f"Error dropping table (proceeding anyway): {e}")

    # --- STEP 2: Create the table from scratch ---
    logging.info(f"Creating {CAND_TABLE}...")
    q = f"""
    CREATE OR REPLACE TABLE `{CAND_TABLE}`
    PARTITION BY DATE(selection_run_ts)
    CLUSTER BY ticker, option_type
    AS
    WITH params AS (
      SELECT
        7     AS min_dte,
        90    AS max_dte,
        1.05  AS min_mny_call,
        1.10  AS max_mny_call,
        1.05  AS min_mny_put,
        1.10  AS max_mny_put,
        500   AS min_oi,
        100   AS min_vol,
        0.10  AS max_spread
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
        FROM `{price_table}` p
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
      CROSS JOIN params p
      WHERE e.dte BETWEEN p.min_dte AND p.max_dte
        AND (
              (e.option_type_lc = 'call'
               AND e.mny_call BETWEEN p.min_mny_call AND p.max_mny_call)
            OR (e.option_type_lc = 'put'
               AND e.mny_put  BETWEEN p.min_mny_put  AND p.max_mny_put)
        )
        AND e.oi_nz >= p.min_oi
        AND e.vol_nz >= p.min_vol
        AND e.spread_pct IS NOT NULL AND e.spread_pct <= p.max_spread
    ),
    normalized AS (
      SELECT
        f.*,
        SAFE_DIVIDE(
          ABS(delta) - MIN(ABS(delta)) OVER (PARTITION BY ticker, option_type_lc),
          NULLIF(
            MAX(ABS(delta)) OVER (PARTITION BY ticker, option_type_lc)
            - MIN(ABS(delta)) OVER (PARTITION BY ticker, option_type_lc),
            0
          )
        ) AS nd,
        SAFE_DIVIDE(
          (MAX(ABS(theta)) OVER (PARTITION BY ticker, option_type_lc) - ABS(theta)),
          NULLIF(
            MAX(ABS(theta)) OVER (PARTITION BY ticker, option_type_lc)
            - MIN(ABS(theta)) OVER (PARTITION BY ticker, option_type_lc),
            0
          )
        ) AS it,
        SAFE_DIVIDE(
          ABS(gamma) - MIN(ABS(gamma)) OVER (PARTITION BY ticker, option_type_lc),
          NULLIF(
            MAX(ABS(gamma)) OVER (PARTITION BY ticker, option_type_lc)
            - MIN(ABS(gamma)) OVER (PARTITION BY ticker, option_type_lc),
            0
          )
        ) AS ng,
        SAFE_DIVIDE(
          (LOG10(1 + vol_nz) + LOG10(1 + oi_nz))
          - MIN((LOG10(1 + vol_nz) + LOG10(1 + oi_nz)))
              OVER (PARTITION BY ticker, option_type_lc),
          NULLIF(
            MAX((LOG10(1 + vol_nz) + LOG10(1 + oi_nz)))
              OVER (PARTITION BY ticker, option_type_lc)
            - MIN((LOG10(1 + vol_nz) + LOG10(1 + oi_nz)))
              OVER (PARTITION BY ticker, option_type_lc),
            0
          )
        )
        * (1 - LEAST(spread_pct, 0.20)/0.20) AS ls,
        SAFE_DIVIDE(
          ABS(delta) * uprice,
          NULLIF(mid_px, 0)
        ) AS elasticity_raw
      FROM filtered f
    ),
    elasticity_norm AS (
      SELECT
        n.*,
        SAFE_DIVIDE(
          elasticity_raw - MIN(elasticity_raw) OVER (PARTITION BY ticker, option_type_lc),
          NULLIF(
            MAX(elasticity_raw) OVER (PARTITION BY ticker, option_type_lc)
            - MIN(elasticity_raw) OVER (PARTITION BY ticker, option_type_lc),
            0
          )
        ) AS ne
      FROM normalized n
    ),
    ranked AS (
      SELECT
        e.*,
        LEAST(
            0.35*COALESCE(nd, 0.5)          
            +0.30*COALESCE(ne, 0.5)         
            +0.15*COALESCE(it, 0.5)         
            +0.15*COALESCE(ls, 0.5)         
            +0.05*COALESCE(ng, 0.5)
            + (CASE WHEN is_uoa THEN 0.1 ELSE 0 END), 
            1.0
        ) AS options_score,
        ROW_NUMBER() OVER (
          PARTITION BY ticker, option_type_lc
          ORDER BY
            (0.35*COALESCE(nd, 0.5) + 0.30*COALESCE(ne, 0.5) + 0.15*COALESCE(it, 0.5) 
             + 0.15*COALESCE(ls, 0.5) + 0.05*COALESCE(ng, 0.5) 
             + (CASE WHEN is_uoa THEN 0.1 ELSE 0 END)) DESC,
            vol_nz DESC,
            oi_nz  DESC
        ) AS rn
      FROM elasticity_norm e
    )
    SELECT
      CURRENT_TIMESTAMP() AS selection_run_ts,
      ticker, signal,
      contract_symbol, option_type, expiration_date, strike,
      last_price, bid, ask, volume, open_interest, implied_volatility,
      delta, theta, vega, gamma, underlying_price, fetch_date,
      options_score, rn, is_uoa
    FROM ranked
    -- NO ORDER BY allowed here during creation/partitioning
    """

    job = bq.query(q)
    job.result()
    logging.info("Created %s with %s rows.", CAND_TABLE, job.num_dml_affected_rows or "unknown")


def run_pipeline(bq_client: bigquery.Client | None = None):
    logging.info("--- Starting Options Candidate Selector (UOA + Elasticity) ---")
    bq = bq_client or bigquery.Client(project=PROJECT)
    
    _create_candidates_table(bq)
    
    logging.info("--- Options Candidate Selector Finished ---")