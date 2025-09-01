import logging
from google.cloud import bigquery
from .. import config

PROJECT = config.PROJECT_ID
DATASET = config.BIGQUERY_DATASET

SCORES_TABLE = f"{PROJECT}.{DATASET}.analysis_scores"
CHAIN_TABLE  = f"{PROJECT}.{DATASET}.options_chain"
CAND_TABLE   = f"{PROJECT}.{DATASET}.options_candidates"

# Optional: make the score threshold easy to tweak
MIN_SCORE = 0.62

def _truncate(bq: bigquery.Client, table_id: str):
    bq.query(f"TRUNCATE TABLE `{table_id}`").result()
    logging.info("Truncated %s", table_id)

def _insert_candidates(bq: bigquery.Client):
    """
    BUY-only flow:
      • Universe = all tickers from the latest run_date with weighted_score > MIN_SCORE
      • Chains = latest fetch_date per ticker (your ingestion already CALL-only)
      • Rank & select top-5 per ticker
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
        7    AS min_dte,
        90   AS max_dte,
        1.05 AS min_mny,
        1.10 AS max_mny,
        500  AS min_oi,
        0.10 AS max_spread
    ),
    -- Latest analysis run_date with scores
    latest_run AS (
      SELECT MAX(run_date) AS run_date
      FROM `{SCORES_TABLE}`
      WHERE weighted_score IS NOT NULL
    ),
    -- BUY universe: all tickers above threshold from latest run_date
    buy_universe AS (
      SELECT s.ticker, s.weighted_score
      FROM `{SCORES_TABLE}` s
      JOIN latest_run r ON s.run_date = r.run_date
      WHERE s.weighted_score > {MIN_SCORE}
    ),
    -- Latest options snapshot per ticker (your ingestion writes fetch_date daily)
    latest_chain_per_ticker AS (
      SELECT ticker, MAX(fetch_date) AS fetch_date
      FROM `{CHAIN_TABLE}`
      GROUP BY ticker
    ),
    -- Latest reliable close prior to (or on) fetch_date, per ticker
    px_latest AS (
      SELECT ticker, adj_close
      FROM (
        SELECT
          p.ticker,
          p.adj_close,
          ROW_NUMBER() OVER (PARTITION BY p.ticker ORDER BY p.date DESC) AS rn
        FROM `{price_table}` p
        JOIN latest_chain_per_ticker l USING (ticker)
        WHERE p.date <= l.fetch_date
      )
      WHERE rn = 1
    ),
    -- Scope the chain to latest snapshot per ticker
    chain_scoped AS (
      SELECT t.*
      FROM `{CHAIN_TABLE}` t
      JOIN latest_chain_per_ticker l USING (ticker, fetch_date)
    ),
    -- Join: only BUY universe; your ingestion already filtered to CALLs, but we keep guardrails
    base AS (
      SELECT
        c.*,
        'BUY' AS signal,
        CASE
          WHEN c.bid > 0 AND c.ask > 0 THEN (c.bid + c.ask) / 2.0
          WHEN c.last_price > 0        THEN c.last_price
          ELSE NULL
        END AS mid_px,
        COALESCE(c.underlying_price, px.adj_close) AS uprice
      FROM chain_scoped c
      JOIN buy_universe u USING (ticker)
      LEFT JOIN px_latest px USING (ticker)
    ),
    enriched AS (
      SELECT
        b.*,
        DATE_DIFF(b.expiration_date, b.fetch_date, DAY) AS dte,
        SAFE_DIVIDE(b.ask - b.bid, NULLIF(b.mid_px, 0)) AS spread_pct,
        LOWER(b.option_type) AS option_type_lc,
        COALESCE(b.open_interest, 0) AS oi_nz,
        COALESCE(b.volume, 0) AS vol_nz,
        -- For BUY calls, moneyness = strike / price ~ 1.05..1.10
        SAFE_DIVIDE(b.strike, NULLIF(b.uprice, 0)) AS moneyness_ratio
      FROM base b
    ),
    filtered AS (
      SELECT e.*
      FROM enriched e, params p
      WHERE e.dte BETWEEN p.min_dte AND p.max_dte
        AND e.option_type_lc = 'call'
        AND e.moneyness_ratio BETWEEN p.min_mny AND p.max_mny
        AND e.oi_nz >= p.min_oi
        AND e.spread_pct IS NOT NULL AND e.spread_pct <= p.max_spread
    ),
    prepared AS (
      SELECT f.*, 0.5 AS iv_percentile   -- TODO: replace with true IV%ile when available
      FROM filtered f
    ),
    normalized AS (
      SELECT
        p.*,
        SAFE_DIVIDE(ABS(delta) - MIN(ABS(delta)) OVER (PARTITION BY ticker),
                    NULLIF(MAX(ABS(delta)) OVER (PARTITION BY ticker)
                         - MIN(ABS(delta)) OVER (PARTITION BY ticker), 0)) AS nd,
        SAFE_DIVIDE((MAX(ABS(theta)) OVER (PARTITION BY ticker) - ABS(theta)),
                    NULLIF(MAX(ABS(theta)) OVER (PARTITION BY ticker)
                         - MIN(ABS(theta)) OVER (PARTITION BY ticker), 0)) AS it,
        SAFE_DIVIDE(ABS(gamma) - MIN(ABS(gamma)) OVER (PARTITION BY ticker),
                    NULLIF(MAX(ABS(gamma)) OVER (PARTITION BY ticker)
                         - MIN(ABS(gamma)) OVER (PARTITION BY ticker), 0)) AS ng,
        SAFE_DIVIDE(
          (LOG10(1 + vol_nz) + LOG10(1 + oi_nz))
          - MIN((LOG10(1 + vol_nz) + LOG10(1 + oi_nz)))
              OVER (PARTITION BY ticker),
          NULLIF(
            MAX((LOG10(1 + vol_nz) + LOG10(1 + oi_nz)))
              OVER (PARTITION BY ticker)
            - MIN((LOG10(1 + vol_nz) + LOG10(1 + oi_nz)))
              OVER (PARTITION BY ticker), 0)
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
          PARTITION BY ticker
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
      ticker, 'BUY' AS signal,
      contract_symbol, option_type, expiration_date, strike,
      last_price, bid, ask, volume, open_interest, implied_volatility,
      delta, theta, vega, gamma, underlying_price, fetch_date,
      options_score, rn
    FROM ranked
    WHERE rn <= 5
    ORDER BY ticker, rn
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
