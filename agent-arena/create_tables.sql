-- Agent Arena BigQuery Tables
-- Project: profitscout-fida8
-- Dataset: profit_scout

-- Individual agent picks (Round 1 initial + Round 4 final)
CREATE TABLE IF NOT EXISTS `profitscout-fida8.profit_scout.agent_arena_picks` (
  scan_date STRING NOT NULL,
  round INT64 NOT NULL,           -- 1 = initial pick, 4 = final pick
  agent_id STRING NOT NULL,       -- claude, gpt, grok, gemini, deepseek, llama, mistral
  ticker STRING NOT NULL,
  direction STRING,               -- bull / bear
  conviction INT64,               -- 1-10
  contract STRING,                -- e.g. "$950C March 21"
  reasoning STRING,
  -- Performance tracking (filled by win tracker later)
  close_price_at_pick FLOAT64,
  close_price_1d FLOAT64,
  close_price_3d FLOAT64,
  close_price_5d FLOAT64,
  return_1d FLOAT64,
  return_3d FLOAT64,
  return_5d FLOAT64,
  win_1d BOOL,
  win_3d BOOL,
  win_5d BOOL,
  created_at TIMESTAMP
);

-- Daily consensus results
CREATE TABLE IF NOT EXISTS `profitscout-fida8.profit_scout.agent_arena_consensus` (
  scan_date STRING NOT NULL,
  has_consensus BOOL,
  consensus_ticker STRING,
  consensus_direction STRING,
  consensus_agent_count INT64,
  consensus_avg_conviction FLOAT64,
  total_agents INT64,
  total_unique_tickers INT64,
  unanimous_count INT64,
  supermajority_count INT64,
  majority_count INT64,
  split_count INT64,
  solo_count INT64,
  duration_seconds FLOAT64,
  -- Performance tracking
  consensus_return_1d FLOAT64,
  consensus_return_3d FLOAT64,
  consensus_return_5d FLOAT64,
  consensus_win_1d BOOL,
  consensus_win_3d BOOL,
  created_at TIMESTAMP
);

-- Full debate rounds (raw text for research)
CREATE TABLE IF NOT EXISTS `profitscout-fida8.profit_scout.agent_arena_rounds` (
  scan_date STRING NOT NULL,
  round INT64 NOT NULL,           -- 1, 2, 3, 4
  round_type STRING,              -- picks, cross_examination, defense, final_vote
  agent_id STRING NOT NULL,
  raw_response STRING,            -- Full model response text
  parsed_json STRING,             -- Parsed JSON string
  created_at TIMESTAMP
);
