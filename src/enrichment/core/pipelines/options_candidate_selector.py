import logging
from google.cloud import bigquery
from .. import config

PROJECT = config.PROJECT_ID
DATASET = config.BIGQUERY_DATASET

SCORES_TABLE = f"{PROJECT}.{DATASET}.analysis_scores"
CHAIN_TABLE = f"{PROJECT}.{DATASET}.options_chain"
CAND_TABLE = f"{PROJECT}.{DATASET}.options_candidates"


def _truncate(bq: bigquery.Client, table_id: str):
    bq.query(f"TRUNCATE TABLE `{table_id}`").result()
    logging.info("Truncated %s", table_id)


def _insert_candidates(bq: bigquery.Client):
    """
    Tightened window with minimal changes:
      • DTE: 10–60 (tilts toward better expectancy for long options)
      • Moneyness: calls strike/price ∈ [1.02, 1.10], puts price/strike ∈ [1.02, 1.10]
      • Liquidity: OI ≥ 250 (MODIFIED), Volume >= 20 (MODIFIED), tighter spread cap 15% (MODIFIED)
      • Price floor: mid_px ≥ $0.50 (avoid micro-premiums with punitive % spreads)
      • Delta band: |delta| ∈ [0.25, 0.45]
      • Edge realism: breakeven_distance_pct ≤ expected_move_pct (IV × sqrt(DTE/365) × 0.85)
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
        10    AS min_dte,      -- was 7
        60    AS max_dte,      -- was 120
        1.02  AS min_mny_call,
        1.10  AS max_mny_call, -- was 1.15
        1.02  AS min_mny_put,
        1.10  AS max_mny_put,  -- was 1.15
        250   AS min_oi,       -- MODIFIED: Was 300
        20    AS min_vol,      -- MODIFIED: Was 50
        0.15  AS max_spread,   -- MODIFIED: Was 0.12 (15%)
        0.50  AS min_mid,      -- new: price floor ($)
        0.25  AS min_abs_delta,
        0.45  AS max_abs_delta,
        0.85  AS exp_move_haircut
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
        COALESCE(b.volume, 0) AS vol_nz, -- Use vol_nz here
        SAFE_DIVIDE(b.strike, NULLIF(b.uprice, 0)) AS mny_call,
        SAFE_DIVIDE(b.uprice, NULLIF(b.strike, 0)) AS mny_put
      FROM base b
    ),
    -- Edge math: expected move and breakeven distance (percent of underlying)
    edge AS (
      SELECT
        e.*,
        -- expected move % over DTE using annual IV (haircut for realism)
        (e.implied_volatility * SQRT(SAFE_DIVIDE(e.dte, 365.0)) * p.exp_move_haircut) * 100.0
          AS expected_move_pct,
        CASE
          WHEN e.option_type_lc = 'call' AND e.mid_px IS NOT NULL AND e.uprice > 0
            THEN SAFE_MULTIPLY(
                   SAFE_DIVIDE((e.strike + e.mid_px) - e.uprice, NULLIF(e.uprice, 0)),
                   100.0)
          WHEN e.option_type_lc = 'put'  AND e.mid_px IS NOT NULL AND e.uprice > 0
            THEN SAFE_MULTIPLY(
                   SAFE_DIVIDE(e.uprice - (e.strike - e.mid_px), NULLIF(e.uprice, 0)),
                   100.0)
          ELSE NULL
        END AS breakeven_distance_pct
      FROM enriched e
      CROSS JOIN params p
    ),
    filtered AS (
      SELECT x.*,
             CASE WHEN x.option_type_lc = 'call' THEN 'BUY' ELSE 'SELL' END AS signal
      FROM edge x
      CROSS JOIN params p
      WHERE x.dte BETWEEN p.min_dte AND p.max_dte
        AND x.mid_px IS NOT NULL AND x.mid_px >= p.min_mid
        AND ABS(x.delta) BETWEEN p.min_abs_delta AND p.max_abs_delta
        AND (
              (x.option_type_lc = 'call'
               AND x.mny_call BETWEEN p.min_mny_call AND p.max_mny_call)
           OR (x.option_type_lc = 'put'
               AND x.mny_put  BETWEEN p.min_mny_put  AND p.max_mny_put)
            )
        AND x.oi_nz >= p.min_oi
        AND x.vol_nz >= p.min_vol -- MODIFIED filter for volume
        AND x.spread_pct IS NOT NULL AND x.spread_pct <= p.max_spread
        AND x.expected_move_pct IS NOT NULL
        AND x.breakeven_distance_pct IS NOT NULL
        AND x.breakeven_distance_pct <= x.expected_move_pct  -- edge realism
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