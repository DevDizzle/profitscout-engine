# PROMPT: Build Momentum Scanner Cloud Function

## Context
We have an existing options pipeline (`options_workflow`) that runs at 9:35 AM EST and produces a scored watchlist in `options_candidates` and `options_analysis_signals` tables. The problem: by the time we see picks, the momentum pop has already happened. We need a **second snapshot** 30 minutes later that compares real-time data to the morning snapshot to confirm momentum is actually happening.

## What to Build

### New Cloud Function: `momentum-scanner`
**Location:** `src/enrichment/core/pipelines/momentum_scanner.py`
**Cloud Function entry:** Add to `src/enrichment/main.py` as `run-momentum-scanner`

### Logic

#### Step 1: Load Morning Watchlist
```sql
-- Get top candidates from the morning run (Snapshot A)
SELECT 
    c.ticker,
    c.contract_symbol,
    c.option_type,
    c.strike,
    c.expiration_date,
    c.last_price AS snapshot_a_price,
    c.bid AS snapshot_a_bid,
    c.ask AS snapshot_a_ask,
    c.volume AS snapshot_a_volume,
    c.open_interest AS snapshot_a_oi,
    c.implied_volatility AS snapshot_a_iv,
    c.underlying_price AS snapshot_a_underlying,
    c.options_score,
    c.signal,
    s.setup_quality_signal,
    s.volatility_comparison_signal,
    s.stock_price_trend_signal
FROM `{PROJECT}.{DATASET}.options_candidates` c
JOIN `{PROJECT}.{DATASET}.options_analysis_signals` s 
    ON c.contract_symbol = s.contract_symbol
WHERE c.selection_run_ts = (SELECT MAX(selection_run_ts) FROM `{PROJECT}.{DATASET}.options_candidates`)
    AND c.rn = 1  -- Best contract per ticker/side
    AND s.setup_quality_signal IN ('Strong', 'Fair')
    AND (
        (c.signal = 'BUY' AND s.stock_price_trend_signal IN ('Strongly Bullish', 'Moderately Bullish'))
        OR (c.signal = 'SELL' AND s.stock_price_trend_signal IN ('Strongly Bearish', 'Moderately Bearish'))
    )
ORDER BY c.options_score DESC
LIMIT 20
```

#### Step 2: Re-fetch Current Data from Polygon
For each ticker in the watchlist:
- Use existing `PolygonClient.fetch_options_chain()` but ONLY for the specific contract symbols from Step 1
- OR use Polygon's snapshot endpoint for faster single-contract lookups: `GET /v3/snapshot/options/{underlyingAsset}/{optionContract}`
- Also fetch current underlying price: `GET /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}`

#### Step 3: Compare Snapshots and Score Momentum
For each contract, calculate:

```python
# Premium momentum
snapshot_a_mid = (snapshot_a_bid + snapshot_a_ask) / 2
snapshot_b_mid = (current_bid + current_ask) / 2
premium_change_pct = (snapshot_b_mid - snapshot_a_mid) / snapshot_a_mid * 100

# Volume spike
volume_ratio = current_volume / max(snapshot_a_volume, 1)

# Underlying direction confirmation
underlying_change_pct = (current_underlying - snapshot_a_underlying) / snapshot_a_underlying * 100
direction_confirmed = (
    (signal == 'BUY' and underlying_change_pct > 0.2) or  # Stock up for calls
    (signal == 'SELL' and underlying_change_pct < -0.2)    # Stock down for puts
)

# Momentum Score
momentum_score = 0
if premium_change_pct > 5:    momentum_score += 3   # Premium ripping
elif premium_change_pct > 2:  momentum_score += 2   # Premium up
elif premium_change_pct > 0:  momentum_score += 1   # Slight up

if volume_ratio >= 3:         momentum_score += 3   # Volume exploding
elif volume_ratio >= 2:       momentum_score += 2   # Volume solid
elif volume_ratio >= 1.5:     momentum_score += 1   # Volume building

if direction_confirmed:       momentum_score += 2   # Underlying confirming

# IV check - don't chase if IV already spiked too much
iv_change = current_iv - snapshot_a_iv
if iv_change > 0.15:         momentum_score -= 2   # IV crush risk on entry

# SIGNAL THRESHOLDS
# momentum_score >= 6: STRONG MOMENTUM (enter with confidence)
# momentum_score >= 4: MODERATE MOMENTUM (enter with smaller size)
# momentum_score < 4:  NO ENTRY (momentum not confirmed)
```

#### Step 4: Write Results
Create/truncate table `{PROJECT}.{DATASET}.momentum_signals`:

| Column | Type | Description |
|--------|------|-------------|
| scan_ts | TIMESTAMP | When this scan ran |
| ticker | STRING | Stock ticker |
| contract_symbol | STRING | Options contract |
| option_type | STRING | call/put |
| signal | STRING | BUY/SELL |
| strike | FLOAT | Strike price |
| expiration_date | DATE | Expiry |
| snapshot_a_mid | FLOAT | Morning mid-price |
| snapshot_b_mid | FLOAT | Current mid-price |
| premium_change_pct | FLOAT | % change in premium |
| snapshot_a_volume | INT | Morning volume |
| current_volume | INT | Current volume |
| volume_ratio | FLOAT | Current/morning volume |
| snapshot_a_underlying | FLOAT | Morning stock price |
| current_underlying | FLOAT | Current stock price |
| underlying_change_pct | FLOAT | % change in underlying |
| direction_confirmed | BOOL | Is underlying moving right way? |
| iv_change | FLOAT | Change in IV |
| momentum_score | INT | Composite momentum score |
| momentum_signal | STRING | STRONG/MODERATE/NO_ENTRY |
| options_score | FLOAT | Original pipeline score |
| setup_quality | STRING | From morning analysis |

#### Step 5: Return top results as JSON response
Return the top 5 by momentum_score so the MCP server / alerts can consume it.

### Deployment
1. Add function to `src/enrichment/main.py`:
```python
@functions_framework.http
def run_momentum_scanner(request):
    from .core.pipelines.momentum_scanner import run_pipeline
    run_pipeline()
    return "OK"
```

2. Deploy as Cloud Function: `momentum-scanner`
3. Add to a NEW workflow `workflows/momentum_workflow.yaml` that runs ONLY this function (it's fast, ~2 min)

### Files to Modify
- **NEW:** `src/enrichment/core/pipelines/momentum_scanner.py` — Main logic
- **EDIT:** `src/enrichment/main.py` — Add HTTP entry point
- **NEW:** `workflows/momentum_workflow.yaml` — Simple single-step workflow
- **EDIT:** `src/ingestion/core/clients/polygon_client.py` — Add snapshot endpoint method if not present

### Key Constraints
- Use existing `PolygonClient` from `src/ingestion/core/clients/polygon_client.py`
- Use existing BigQuery patterns from other pipelines
- Keep it fast — only fetching ~20 tickers, should complete in <2 minutes
- Polygon rate limits: use ThreadPoolExecutor(max_workers=5) to be safe with snapshots
- All config from existing `config.py` — don't hardcode project IDs

### Testing
After deployment, the flow is:
1. 9:35 AM EST: Trigger `options_workflow` (existing)
2. 10:05 AM EST: Trigger `momentum_workflow` (new)
3. Check `momentum_signals` table for results
4. Top entries with momentum_score >= 6 are the trades

### GCS/Polygon References
- Polygon API key is in config: `config.POLYGON_API_KEY`
- Snapshot endpoint docs: https://polygon.io/docs/options/get_v3_snapshot_options__underlyingasset___optioncontract
- Stock snapshot: https://polygon.io/docs/stocks/get_v2_snapshot_locale_us_markets_stocks_tickers__stocksticker

## COMPLETION MARKER (REQUIRED)
When ALL tasks are complete (code written, function added to main.py, workflow created), write a file to the ROOT of this repository:

**Filename:** `MOMENTUM_SCANNER_COMPLETE.md`

Include in that file:
- ✅ or ❌ status for each file created/modified
- List of files changed
- Any issues encountered
- Deployment command to run (do NOT deploy — just document the command)

This file is how we verify the task is done. Do not skip this step.
