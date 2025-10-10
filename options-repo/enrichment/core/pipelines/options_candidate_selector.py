import logging
from google.cloud import bigquery
from .. import config

PROJECT = config.PROJECT_ID
DATASET = config.BIGQUERY_DATASET

SCORES_TABLE = f"{PROJECT}.{DATASET}.analysis_scores"
CHAIN_TABLE  = f"{PROJECT}.{DATASET}.options_chain"
CAND_TABLE   = f"{PROJECT}.{DATASET}.options_candidates"

def _truncate(bq: bigquery.Client, table_id: str):
    bq.query(f"TRUNCATE TABLE `{table_id}`").result()
    logging.info("Truncated %s", table_id)

def _insert_candidates(bq: bigquery.Client):
    """
    CALL + PUT filtering & scoring (no BUY/SELL universe gating, no top-5 cap):
      • Chain source = latest fetch_date per ticker from options_chain
      • Resolve underlying price with latest adj_close from price_data (max date)
      • Moneyness (direction-aware):
            - Calls: strike / price in [1.05, 1.10]
            - Puts : price / strike in [1.05, 1.10]
      • Keep DTE, OI, spread filters
      • Score/normalize as before
      • Compute rn (per ticker, option_type) but DO NOT filter by rn
      • Insert all filtered, scored candidates
    """
    price_table = f"{PROJECT}.{DATASET}.price_data"

    q = f"""
    INSERT INTO `{CAND_TABLE}` (
      selection_run_ts, ticker, signal,
      contract_symbol, option_type, expiration_date, strike,
      last_price, bid, ask, volume, open_interest, implied_volatility,
      delta, theta, vega, gamma, underlying_price, fetch_date,
      options_score, rn
    )
    WITH params AS (
      SELECT
        7     AS min_dte,
        90    AS max_dte,
        1.05  AS min_mny_call,
        1.10  AS max_mny_call,
        1.05  AS min_mny_put,
        1.10  AS max_mny_put,
        500   AS min_oi,
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
        COALESCE(b.volume, 0) AS vol_nz,
        SAFE_DIVIDE(b.strike, NULLIF(b.uprice, 0)) AS mny_call,
        SAFE_DIVIDE(b.uprice, NULLIF(b.strike, 0)) AS mny_put
      FROM base b
    ),
    filtered AS (
      SELECT e.*,
             CASE WHEN e.option_type_lc = 'call' THEN 'BUY' ELSE 'SELL' END AS signal
      FROM enriched e
      CROSS JOIN params p -- <<< THIS IS THE FIX: Changed to explicit CROSS JOIN
      WHERE e.dte BETWEEN p.min_dte AND p.max_dte
        AND (
              (e.option_type_lc = 'call'
               AND e.mny_call BETWEEN p.min_mny_call AND p.max_mny_call)
           OR (e.option_type_lc = 'put'
               AND e.mny_put  BETWEEN p.min_mny_put  AND p.max_mny_put)
         )
        AND e.oi_nz >= p.min_oi
        AND e.spread_pct IS NOT NULL AND e.spread_pct <= p.max_spread
    ),
    prepared AS (
      SELECT f.*, 0.5 AS iv_percentile
      FROM filtered f
    ),
    normalized AS (
      SELECT
        p.*,
        SAFE_DIVIDE(ABS(delta) - MIN(ABS(delta)) OVER (PARTITION BY ticker, option_type_lc),
                    NULLIF(MAX(ABS(delta)) OVER (PARTITION BY ticker, option_type_lc)
                         - MIN(ABS(delta)) OVER (PARTITION BY ticker, option_type_lc), 0)) AS nd,
        SAFE_DIVIDE((MAX(ABS(theta)) OVER (PARTITION BY ticker, option_type_lc) - ABS(theta)),
                    NULLIF(MAX(ABS(theta)) OVER (PARTITION BY ticker, option_type_lc)
                         - MIN(ABS(theta)) OVER (PARTITION BY ticker, option_type_lc), 0)) AS it,
        SAFE_DIVIDE(ABS(gamma) - MIN(ABS(gamma)) OVER (PARTITION BY ticker, option_type_lc),
                    NULLIF(MAX(ABS(gamma)) OVER (PARTITION BY ticker, option_type_lc)
                         - MIN(ABS(gamma)) OVER (PARTITION BY ticker, option_type_lc), 0)) AS ng,
        SAFE_DIVIDE(
          (LOG10(1 + vol_nz) + LOG10(1 + oi_nz))
          - MIN((LOG10(1 + vol_nz) + LOG10(1 + oi_nz)))
            OVER (PARTITION BY ticker, option_type_lc),
          NULLIF(
            MAX((LOG10(1 + vol_nz) + LOG10(1 + oi_nz)))
              OVER (PARTITION BY ticker, option_type_lc)
            - MIN((LOG10(1 + vol_nz) + LOG10(1 + oi_nz)))
              OVER (PARTITION BY ticker, option_type_lc), 0)
        ) * (1 - LEAST(spread_pct, 0.20)/0.20) AS ls
      FROM prepared p
    ),
    ranked AS (
      SELECT
        n.*,
        0.35*COALESCE(nd, 0.5)
        +0.25*COALESCE(it, 0.5)
        +0.20*COALESCE(ls, 0.5)
        +0.10*(1 - COALESCE(iv_percentile, 0.5))
        +0.10*COALESCE(ng, 0.5) AS options_score,
        ROW_NUMBER() OVER (
          PARTITION BY ticker, option_type_lc
          ORDER BY
            (0.35*COALESCE(nd, 0.5)
            +0.25*COALESCE(it, 0.5)
            +0.20*COALESCE(ls, 0.5)
            +0.10*(1 - COALESCE(iv_percentile, 0.5))
            +0.10*COALESCE(ng, 0.5)) DESC,
            vol_nz DESC, oi_nz DESC
        ) AS rn
      FROM normalized n
    )
    SELECT
      CURRENT_TIMESTAMP() AS selection_run_ts,
      ticker, signal,
      contract_symbol, option_type, expiration_date, strike,
      last_price, bid, ask, volume, open_interest, implied_volatility,
      delta, theta, vega, gamma, uprice AS underlying_price, fetch_date,
      options_score, rn
    FROM ranked
    ORDER BY ticker, signal, rn
    ;
    """
    bq.query(q).result()

def run_pipeline(bq_client: bigquery.Client | None = None, truncate_table: bool = True):
    logging.info("--- Starting Options Candidate Selector (no LLM) ---")
    bq = bq_client or bigquery.Client(project=PROJECT)
    if truncate_table:
        _truncate(bq, CAND_TABLE)
    _insert_candidates(bq)
    logging.info("--- Options Candidate Selector Finished ---")
