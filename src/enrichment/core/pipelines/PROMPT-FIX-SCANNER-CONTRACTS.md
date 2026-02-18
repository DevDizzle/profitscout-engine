# PROMPT: Fix Scanner Contract Selection

## Problem
The `_best_contract()` function in `overnight_scanner.py` returns `None` for 73% of signals (54/74 on Feb 14 scan). The filters are too strict:
- Moneyness: 0.95-1.10 (misses OTM flow where institutions position)
- Volume: ≥100 (misses early accumulation)
- Spread: ≤20% (valid but kills wide-spread names)
- DTE: 14-45 days only (misses weekly and longer-dated)

This means `recommended_contract`, `recommended_strike`, `recommended_expiration`, `recommended_mid_price`, `recommended_spread_pct`, `contract_score`, `recommended_dte` are ALL null for most signals. The Agent Arena needs real contracts with Greeks to debate.

## Fix Required

### 1. Loosen `_best_contract()` filters in `overnight_scanner.py` (line ~135)

```python
def _best_contract(contracts: list[dict], direction: str, underlying_price: float) -> dict | None:
    """Find the single best contract to trade."""
    candidates = []
    today = date.today()

    for c in contracts:
        strike = c.get("strike") or 0
        exp_str = c.get("expiration_date")
        if not exp_str:
            continue
        try:
            exp_date = date.fromisoformat(str(exp_str)[:10])
        except (ValueError, TypeError):
            continue

        dte = (exp_date - today).days
        if not (7 <= dte <= 90):  # WIDENED from 14-45 to 7-90
            continue

        vol = c.get("volume") or 0
        bid = c.get("bid") or 0
        ask = c.get("ask") or 0
        if bid <= 0 or ask <= 0:
            continue
        mid = (bid + ask) / 2
        if mid <= 0:
            continue

        spread_pct = (ask - bid) / mid
        if spread_pct > 0.40:  # LOOSENED from 0.20 to 0.40
            continue
        if vol < 10:  # LOWERED from 100 to 10
            continue

        oi = c.get("open_interest") or 0

        # Moneyness filter — WIDENED to capture OTM institutional flow
        if underlying_price and underlying_price > 0:
            if direction == "call":
                mny = strike / underlying_price
                if not (0.90 <= mny <= 1.25):  # WIDENED from 0.95-1.10
                    continue
            else:
                mny = underlying_price / strike
                if not (0.90 <= mny <= 1.25):  # WIDENED from 0.95-1.10
                    continue

        # Enhanced scoring with Greeks
        delta = abs(c.get("delta") or 0)
        gamma = c.get("gamma") or 0
        iv = c.get("implied_volatility") or 0
        theta = abs(c.get("theta") or 0)
        
        score = (
            min(vol / 500, 5.0) * 2.0          # Volume (scaled down threshold)
            + (1.0 - min(spread_pct, 1.0)) * 3.0  # Tight spreads
            + min(vol / max(oi, 1), 3.0) * 1.5    # Vol/OI ratio
            + gamma * 20.0                          # Gamma exposure
            + (0.5 if 0.25 <= delta <= 0.50 else 0) * 2.0  # Sweet spot delta bonus
            - (theta / max(mid, 0.01)) * 1.0       # Theta drag penalty
        )
        candidates.append({
            "contract_symbol": c.get("contract_symbol"),
            "strike": strike,
            "expiration_date": str(exp_date),
            "dte": dte,
            "mid_price": round(mid, 2),
            "spread_pct": round(spread_pct, 4),
            "volume": vol,
            "open_interest": oi,
            "implied_volatility": round(iv, 4) if iv else None,
            "gamma": round(gamma, 6) if gamma else None,
            "delta": round(c.get("delta") or 0, 4),
            "theta": round(c.get("theta") or 0, 4),
            "vega": round(c.get("vega") or 0, 4),
            "contract_score": round(score, 3),
        })

    if not candidates:
        return None
    return max(candidates, key=lambda x: x["contract_score"])
```

### 2. Update the signal row builder (line ~355) to include Greeks

Currently (line ~377):
```python
"recommended_contract": best.get("contract_symbol") if best else None,
```

Add after the existing contract fields:
```python
"recommended_delta": best.get("delta") if best else None,
"recommended_gamma": best.get("gamma") if best else None,
"recommended_theta": best.get("theta") if best else None,
"recommended_vega": best.get("vega") if best else None,
"recommended_iv": best.get("implied_volatility") if best else None,
"recommended_volume": best.get("volume") if best else None,
"recommended_oi": best.get("open_interest") if best else None,
```

### 3. Add BQ columns for Greeks

Run this DDL:
```sql
ALTER TABLE `profitscout-fida8.profit_scout.overnight_signals` 
  ADD COLUMN IF NOT EXISTS recommended_delta FLOAT64,
  ADD COLUMN IF NOT EXISTS recommended_gamma FLOAT64,
  ADD COLUMN IF NOT EXISTS recommended_theta FLOAT64,
  ADD COLUMN IF NOT EXISTS recommended_vega FLOAT64,
  ADD COLUMN IF NOT EXISTS recommended_iv FLOAT64,
  ADD COLUMN IF NOT EXISTS recommended_volume INT64,
  ADD COLUMN IF NOT EXISTS recommended_oi INT64;
```

### 4. Add to BQ schema definition (line ~413)

```python
bigquery.SchemaField("recommended_delta", "FLOAT"),
bigquery.SchemaField("recommended_gamma", "FLOAT"),
bigquery.SchemaField("recommended_theta", "FLOAT"),
bigquery.SchemaField("recommended_vega", "FLOAT"),
bigquery.SchemaField("recommended_iv", "FLOAT"),
bigquery.SchemaField("recommended_volume", "INTEGER"),
bigquery.SchemaField("recommended_oi", "INTEGER"),
```

## Expected Result
- Contract coverage should go from ~27% to ~90%+
- Each signal will have: specific contract symbol, strike, expiration, mid price, spread, Greeks (delta, gamma, theta, vega, IV), volume, OI
- Agent Arena will receive REAL contracts with actual Greeks to debate

## Deploy
After making changes:
```bash
cd ~/gammarips-engine
gcloud run deploy overnight-scanner --source . --region us-central1 --project profitscout-fida8
```
