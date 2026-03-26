-- GammaRips V3 ledger reset plan
-- Date: 2026-03-26
-- Intent: preserve prior cohort evidence, then create a clean V3 forward ledger target.

-- 1) Snapshot/archive the current ledger if it still contains historical V2 rows.
CREATE OR REPLACE TABLE `profitscout-fida8.profit_scout.forward_paper_ledger_v2_archive_20260326` AS
SELECT *
FROM `profitscout-fida8.profit_scout.forward_paper_ledger`;

-- 2) Create a clean V3 ledger.
CREATE OR REPLACE TABLE `profitscout-fida8.profit_scout.forward_paper_ledger_v3` (
  scan_date DATE NOT NULL,
  ticker STRING NOT NULL,
  recommended_contract STRING NOT NULL,
  direction STRING NOT NULL,
  is_premium_signal BOOL,
  premium_score INT64,

  policy_version STRING,
  policy_gate STRING,
  is_skipped BOOL NOT NULL,
  skip_reason STRING,

  VIX_at_entry FLOAT64,
  SPY_trend_state STRING,
  recommended_dte INT64,
  recommended_volume INT64,
  recommended_oi INT64,
  recommended_spread_pct FLOAT64,

  entry_timestamp TIMESTAMP,
  entry_price FLOAT64,
  target_price FLOAT64,
  stop_price FLOAT64,
  exit_timestamp TIMESTAMP,
  exit_reason STRING,
  realized_return_pct FLOAT64
);

-- 3) Optional compatibility path if code still temporarily writes to the legacy table name.
--    Prefer updating code to use `forward_paper_ledger_v3` directly instead of relying on this.
-- TRUNCATE TABLE `profitscout-fida8.profit_scout.forward_paper_ledger`;

-- 4) Recommended read-only verification queries.
-- SELECT COUNT(*) FROM `profitscout-fida8.profit_scout.forward_paper_ledger_v2_archive_20260326`;
-- SELECT COUNT(*) FROM `profitscout-fida8.profit_scout.forward_paper_ledger_v3`;

-- 5) Suggested policy metadata values for V3 writes.
-- policy_version = 'V3_LIQUIDITY_ONLY'
-- policy_gate    = 'PREMIUM_GTE_2__VOL_GT_250_OR_OI_GT_500'
