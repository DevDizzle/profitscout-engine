# SPEC: Overnight Scanner Scoring v2 - Industry Cluster Boost

## Overview
Add industry-level cluster detection to the scoring pipeline. When 4+ tickers in the same industry flag the same direction overnight, boost under-threshold tickers to catch sector rotation signals.

## Changes

### 1. BQ Schema Update - overnight_signals table
Add columns to `profitscout-fida8.profit_scout.overnight_signals`:
- `sector` (STRING) - e.g. "Technology"
- `industry` (STRING) - e.g. "Semiconductors"  
- `cluster_size` (INTEGER) - number of same-industry, same-direction tickers in this scan
- `cluster_boost` (INTEGER) - points added from cluster (0-3)
- `original_score` (INTEGER) - pre-boost score
- `overnight_score` becomes the boosted score (existing column, no change needed)

### 2. Metadata Backfill
Current: 1,012 tickers in `profitscout-lx6bb.profit_scout.stock_metadata`
Needed: ~2,300+ (all tickers in scan universe)
Source: Polygon Ticker Details API (`/v3/reference/tickers/{ticker}`)
Fields needed: `sic_description` (industry), sector mapping

### 3. Scoring Logic Changes in overnight_scanner.py

#### a. Load metadata at pipeline start
```python
def _load_metadata() -> dict:
    """Load sector/industry mapping from BQ."""
    bq = bigquery.Client(project='profitscout-lx6bb')
    rows = bq.query('SELECT ticker, sector, industry FROM `profitscout-lx6bb.profit_scout.stock_metadata`').result()
    return {r.ticker: {'sector': r.sector, 'industry': r.industry} for r in rows}
```

#### b. Lower thresholds in _score_ticker()
- Vol/OI: >0.8 = 1pt, >2.0 = 2pts (was 1.5/3.0)
- Price Momentum: >1.5% = 1pt (was 2.0%)

#### c. Add cluster boost as post-processing step
After all tickers scored individually:
```python
def _apply_cluster_boost(scored: list[dict], metadata: dict) -> list[dict]:
    """Apply industry cluster boost to under-threshold tickers."""
    # 1. Tag each signal with industry
    for s in scored:
        meta = metadata.get(s['ticker'], {})
        s['sector'] = meta.get('sector')
        s['industry'] = meta.get('industry')
    
    # 2. Count clusters: group by (industry, direction)
    from collections import Counter
    clusters = Counter()
    for s in scored:
        if s['industry']:
            clusters[(s['industry'], s['direction'])] += 1
    
    # 3. Apply boost only to tickers scoring < 6
    for s in scored:
        s['original_score'] = s['overnight_score']
        s['cluster_boost'] = 0
        s['cluster_size'] = 0
        
        if s['industry']:
            key = (s['industry'], s['direction'])
            cluster_size = clusters.get(key, 0)
            s['cluster_size'] = cluster_size
            
            if s['overnight_score'] < 6 and cluster_size >= 4:
                if cluster_size >= 8:
                    boost = 3
                elif cluster_size >= 5:
                    boost = 2
                else:  # 4
                    boost = 1
                
                s['cluster_boost'] = boost
                s['overnight_score'] = min(s['overnight_score'] + boost, 10)
    
    return scored
```

#### d. Pipeline order update in run_pipeline()
```
Step 1: Load universe
Step 2: Load metadata  <-- NEW
Step 3: Pass 1 - stock snapshots
Step 4: Pass 2 - options chains
Step 5: Score individually
Step 6: Apply cluster boost  <-- NEW
Step 7: Write results
```

### 4. BQ Write Update
Add new fields to _write_results() row dict:
- sector, industry, cluster_size, cluster_boost, original_score

### 5. _ensure_table() Schema Update
Add new SchemaField entries for the 5 new columns.

### 6. Metadata Backfill Strategy
Option A (fast, tonight): Use Polygon bulk ticker API to fetch missing tickers
Option B (simpler): Scanner already has ticker list - query Polygon during scan and cache

Going with Option B: during `_load_metadata()`, for any ticker in universe not in BQ, fetch from Polygon and backfill on the fly. Cache in BQ for next run.

Actually simpler: just backfill the metadata table now with a one-time script using Polygon API, then the scanner just reads it.

## Test Plan
1. Deploy updated scanner to Cloud Run
2. Hit `/scan` endpoint manually (or use test mode)
3. Query BQ to verify: new columns populated, cluster_boost > 0 for expected tickers
4. Verify SNDK-type tickers now score >= 6
5. Hit `/signals` endpoint - verify new fields returned

## Files Modified
- `src/enrichment/core/pipelines/overnight_scanner.py` - scoring + cluster logic
- `server_scanner.py` - no changes needed (calls run_pipeline())
- One-time backfill script for metadata
