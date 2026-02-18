# enrichment/core/pipelines/overnight_scanner.py
"""
Overnight Options Flow Scanner v1.
Scans 3,000+ tickers for institutional options positioning.
Pure numeric scoring — zero LLM calls.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

from google.cloud import bigquery, storage

from .. import config
from ..clients.polygon_client import PolygonClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration ---
MIN_PRICE_CHANGE_PCT = float(
    __import__("os").environ.get("MIN_PRICE_CHANGE", "1.0")
)
MIN_DOLLAR_VOLUME = float(
    __import__("os").environ.get("MIN_DOLLAR_VOLUME", "500000")
)
MIN_SCORE = int(__import__("os").environ.get("MIN_SCORE", "6"))
MAX_WORKERS = 16


# =====================================================================
# UNIVERSE
# =====================================================================

def _load_universe() -> set[str]:
    """Load ticker universe from GCS file."""
    try:
        client = storage.Client(project=config.PROJECT_ID)
        bucket = client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(config.OVERNIGHT_UNIVERSE_FILE)
        content = blob.download_as_text(encoding="utf-8")
        tickers = {t.strip().upper() for t in content.splitlines() if t.strip()}
        logger.info("Loaded %d tickers from %s", len(tickers), config.OVERNIGHT_UNIVERSE_FILE)
        return tickers
    except Exception as e:
        logger.error("Failed to load universe from GCS: %s", e)
        return set()


# =====================================================================
# PASS 1: STOCK SNAPSHOTS (one API call)
# =====================================================================

def _pass1_stock_snapshots(poly: PolygonClient, universe: set[str]) -> list[dict]:
    """Fetch all-tickers snapshot, filter to universe movers."""
    logger.info("Pass 1: Fetching all-tickers snapshot...")
    snapshot = poly.fetch_all_tickers_snapshot()

    if not snapshot:
        logger.error("Pass 1: No snapshot data returned.")
        return []

    movers = []
    for item in snapshot:
        ticker = item.get("ticker")
        if ticker not in universe:
            continue

        day = item.get("day") or {}
        change_pct = item.get("todaysChangePerc") or 0.0
        day_vol = day.get("v") or 0
        close_price = day.get("c")

        # Also try lastTrade for price
        if not close_price:
            close_price = (item.get("lastTrade") or {}).get("p")
        # Also try prevDay close as fallback context
        prev_close = (item.get("prevDay") or {}).get("c")

        if abs(change_pct) >= MIN_PRICE_CHANGE_PCT and close_price:
            movers.append({
                "ticker": ticker,
                "todaysChangePerc": round(change_pct, 2),
                "day_volume": int(day_vol) if day_vol else 0,
                "underlying_price": float(close_price),
                "prev_close": float(prev_close) if prev_close else None,
            })

    logger.info("Pass 1: %d movers from %d universe tickers.", len(movers), len(universe))
    return movers


# =====================================================================
# FLOW METRICS
# =====================================================================

def _dollar_vol(contracts: list[dict]) -> float:
    total = 0.0
    for c in contracts:
        mid = None
        bid, ask = c.get("bid"), c.get("ask")
        if bid and ask and bid > 0 and ask > 0:
            mid = (bid + ask) / 2
        elif c.get("last_price"):
            mid = c["last_price"]
        if mid and c.get("volume"):
            total += c["volume"] * mid * 100
    return total


def _count_active_strikes(contracts: list[dict]) -> int:
    return sum(
        1 for c in contracts
        if (c.get("volume") or 0) > max((c.get("open_interest") or 0) * 0.5, 100)
    )


def _uoa_depth(contracts: list[dict]) -> float:
    total = 0.0
    for c in contracts:
        vol = c.get("volume") or 0
        oi = c.get("open_interest") or 0
        if vol > oi:
            mid = None
            bid, ask = c.get("bid"), c.get("ask")
            if bid and ask and bid > 0 and ask > 0:
                mid = (bid + ask) / 2
            elif c.get("last_price"):
                mid = c["last_price"]
            if mid:
                total += (vol - oi) * mid * 100
    return total


def _best_contract(contracts: list[dict], direction: str, underlying_price: float) -> dict | None:
    """Find the single best contract to trade with full Greeks."""
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
        if not (7 <= dte <= 90):
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
        if spread_pct > 0.40:
            continue
        if vol < 10:
            continue

        oi = c.get("open_interest") or 0

        # Moneyness filter — widened for OTM institutional flow
        if underlying_price and underlying_price > 0:
            if direction == "call":
                mny = strike / underlying_price
                if not (0.90 <= mny <= 1.25):
                    continue
            else:
                mny = underlying_price / strike
                if not (0.90 <= mny <= 1.25):
                    continue

        delta = abs(c.get("delta") or 0)
        gamma = c.get("gamma") or 0
        theta = c.get("theta") or 0
        iv = c.get("implied_volatility") or 0

        score = (
            min(vol / 500, 5.0) * 2.0
            + (1.0 - min(spread_pct, 1.0)) * 3.0
            + min(vol / max(oi, 1), 3.0) * 1.5
            + gamma * 20.0
            + (2.0 if 0.25 <= delta <= 0.50 else 0)  # Sweet spot delta bonus
            - (abs(theta) / max(mid, 0.01)) * 1.0     # Theta drag penalty
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
            "theta": round(theta, 4) if theta else None,
            "vega": round(c.get("vega") or 0, 4) if c.get("vega") else None,
            "contract_score": round(score, 3),
        })

    if not candidates:
        return None
    return max(candidates, key=lambda x: x["contract_score"])


def _compute_flow_metrics(chain: list[dict], underlying_price: float) -> dict:
    """Compute aggregate options flow metrics."""
    calls = [c for c in chain if c.get("option_type") == "call"]
    puts = [c for c in chain if c.get("option_type") == "put"]

    call_vol = sum(c.get("volume") or 0 for c in calls)
    call_oi = sum(c.get("open_interest") or 0 for c in calls)
    put_vol = sum(c.get("volume") or 0 for c in puts)
    put_oi = sum(c.get("open_interest") or 0 for c in puts)

    call_dv = _dollar_vol(calls)
    put_dv = _dollar_vol(puts)

    # ATM IV
    def _atm_iv(contracts):
        if not contracts or not underlying_price:
            return None
        nearest = min(contracts, key=lambda c: abs((c.get("strike") or 0) - underlying_price))
        return nearest.get("implied_volatility")

    return {
        "call_dollar_vol": call_dv,
        "put_dollar_vol": put_dv,
        "call_vol_oi": call_vol / max(call_oi, 1),
        "put_vol_oi": put_vol / max(put_oi, 1),
        "call_active_strikes": _count_active_strikes(calls),
        "put_active_strikes": _count_active_strikes(puts),
        "call_uoa_depth": _uoa_depth(calls),
        "put_uoa_depth": _uoa_depth(puts),
        "atm_call_iv": _atm_iv(calls),
        "atm_put_iv": _atm_iv(puts),
        "total_call_volume": call_vol,
        "total_put_volume": put_vol,
        "best_call": _best_contract(calls, "call", underlying_price),
        "best_put": _best_contract(puts, "put", underlying_price),
    }


# =====================================================================
# PASS 2: OPTIONS CHAINS (parallel, only movers)
# =====================================================================

def _process_ticker(poly: PolygonClient, ticker_info: dict) -> dict | None:
    """Pass 2 worker: fetch chain + compute metrics for one mover."""
    ticker = ticker_info["ticker"]
    underlying_price = ticker_info.get("underlying_price") or 0

    try:
        chain = poly.fetch_options_chain(ticker, max_days=45)
        if not chain:
            return None
        metrics = _compute_flow_metrics(chain, underlying_price)
        return {**ticker_info, **metrics}
    except Exception as e:
        logger.error("[%s] Options fetch failed: %s", ticker, e)
        return None


def _pass2_options(poly: PolygonClient, movers: list[dict]) -> list[dict]:
    """Fetch options chains in parallel for all movers."""
    logger.info("Pass 2: Fetching options for %d movers...", len(movers))
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futs = {
            executor.submit(_process_ticker, poly, info): info["ticker"]
            for info in movers
        }
        for fut in as_completed(futs):
            ticker = futs[fut]
            try:
                data = fut.result()
                if data:
                    results.append(data)
            except Exception as e:
                logger.error("[%s] Worker failed: %s", ticker, e)

    logger.info("Pass 2 complete: %d tickers with options data.", len(results))
    return results


# =====================================================================
# SCORING (pure numeric, 0-10)
# =====================================================================

def _score_ticker(data: dict) -> dict:
    """
    Score a ticker based on where money loaded.
    Returns enriched dict with score, direction, signals, best contract.
    """
    score = 0
    signals = []

    price_change_pct = data.get("todaysChangePerc") or 0
    bullish = price_change_pct > 0
    direction = "BULLISH" if bullish else "BEARISH"

    call_dv = data.get("call_dollar_vol") or 0
    put_dv = data.get("put_dollar_vol") or 0
    total_dv = call_dv + put_dv

    # --- SIGNAL 1: Dollar Volume Skew (0-2 pts) ---
    if total_dv > MIN_DOLLAR_VOLUME:
        if bullish and call_dv > 0:
            skew = call_dv / max(put_dv, 1)
            if skew > 3.0:
                score += 2; signals.append(f"Call $ {skew:.1f}x puts")
            elif skew > 1.5:
                score += 1; signals.append(f"Call $ {skew:.1f}x puts")
        elif not bullish and put_dv > 0:
            skew = put_dv / max(call_dv, 1)
            if skew > 3.0:
                score += 2; signals.append(f"Put $ {skew:.1f}x calls")
            elif skew > 1.5:
                score += 1; signals.append(f"Put $ {skew:.1f}x calls")

    # --- SIGNAL 2: Volume/OI Ratio (0-2 pts) ---
    rel_vol_oi = data.get("call_vol_oi", 0) if bullish else data.get("put_vol_oi", 0)
    if rel_vol_oi > 3.0:
        score += 2; signals.append(f"Vol/OI {rel_vol_oi:.1f}x (very unusual)")
    elif rel_vol_oi > 1.5:
        score += 1; signals.append(f"Vol/OI {rel_vol_oi:.1f}x (unusual)")

    # --- SIGNAL 3: Multi-Strike Accumulation (0-2 pts) ---
    rel_strikes = data.get("call_active_strikes", 0) if bullish else data.get("put_active_strikes", 0)
    if rel_strikes >= 5:
        score += 2; signals.append(f"{rel_strikes} strikes active (institutional)")
    elif rel_strikes >= 3:
        score += 1; signals.append(f"{rel_strikes} strikes active")

    # --- SIGNAL 4: UOA Depth (0-2 pts) ---
    rel_uoa = data.get("call_uoa_depth", 0) if bullish else data.get("put_uoa_depth", 0)
    if rel_uoa > 2_000_000:
        score += 2; signals.append(f"${rel_uoa / 1e6:.1f}M new positioning")
    elif rel_uoa > 500_000:
        score += 1; signals.append(f"${rel_uoa / 1e3:.0f}K new positioning")

    # --- SIGNAL 5: Price Momentum (0-1 pt) ---
    if abs(price_change_pct) > 2.0:
        score += 1; signals.append(f"Price moved {price_change_pct:+.1f}%")

    # --- SIGNAL 6: Smart Money Divergence (0-1 pt) ---
    if bullish and put_dv > call_dv * 2 and put_dv > 1_000_000:
        direction = "BEARISH"
        score += 1; signals.append("DIVERGENCE: heavy puts despite rally")
    elif not bullish and call_dv > put_dv * 2 and call_dv > 1_000_000:
        direction = "BULLISH"
        score += 1; signals.append("DIVERGENCE: heavy calls despite selloff")

    # Pick best contract for the determined direction
    best = data.get("best_call") if direction == "BULLISH" else data.get("best_put")

    return {
        "ticker": data["ticker"],
        "direction": direction,
        "overnight_score": score,
        "price_change_pct": price_change_pct,
        "underlying_price": data.get("underlying_price"),
        "day_volume": data.get("day_volume"),
        "call_dollar_volume": call_dv,
        "put_dollar_volume": put_dv,
        "total_options_dollar_volume": total_dv,
        "call_vol_oi_ratio": data.get("call_vol_oi"),
        "put_vol_oi_ratio": data.get("put_vol_oi"),
        "call_active_strikes": data.get("call_active_strikes"),
        "put_active_strikes": data.get("put_active_strikes"),
        "call_uoa_depth": data.get("call_uoa_depth"),
        "put_uoa_depth": data.get("put_uoa_depth"),
        "signals": signals,
        "recommended_contract": best.get("contract_symbol") if best else None,
        "recommended_strike": best.get("strike") if best else None,
        "recommended_expiration": best.get("expiration_date") if best else None,
        "recommended_dte": best.get("dte") if best else None,
        "recommended_mid_price": best.get("mid_price") if best else None,
        "recommended_spread_pct": best.get("spread_pct") if best else None,
        "contract_score": best.get("contract_score") if best else None,
        "recommended_delta": best.get("delta") if best else None,
        "recommended_gamma": best.get("gamma") if best else None,
        "recommended_theta": best.get("theta") if best else None,
        "recommended_vega": best.get("vega") if best else None,
        "recommended_iv": best.get("implied_volatility") if best else None,
        "recommended_volume": best.get("volume") if best else None,
        "recommended_oi": best.get("open_interest") if best else None,
    }


# =====================================================================
# BIGQUERY OUTPUT
# =====================================================================

def _ensure_table(bq: bigquery.Client):
    """Create overnight_signals table if it doesn't exist."""
    table_id = config.OVERNIGHT_SIGNALS_TABLE
    schema = [
        bigquery.SchemaField("scan_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("scan_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("direction", "STRING"),
        bigquery.SchemaField("overnight_score", "INTEGER"),
        bigquery.SchemaField("price_change_pct", "FLOAT"),
        bigquery.SchemaField("underlying_price", "FLOAT"),
        bigquery.SchemaField("day_volume", "INTEGER"),
        bigquery.SchemaField("call_dollar_volume", "FLOAT"),
        bigquery.SchemaField("put_dollar_volume", "FLOAT"),
        bigquery.SchemaField("total_options_dollar_volume", "FLOAT"),
        bigquery.SchemaField("call_vol_oi_ratio", "FLOAT"),
        bigquery.SchemaField("put_vol_oi_ratio", "FLOAT"),
        bigquery.SchemaField("call_active_strikes", "INTEGER"),
        bigquery.SchemaField("put_active_strikes", "INTEGER"),
        bigquery.SchemaField("call_uoa_depth", "FLOAT"),
        bigquery.SchemaField("put_uoa_depth", "FLOAT"),
        bigquery.SchemaField("signals", "STRING", mode="REPEATED"),
        bigquery.SchemaField("recommended_contract", "STRING"),
        bigquery.SchemaField("recommended_strike", "FLOAT"),
        bigquery.SchemaField("recommended_expiration", "DATE"),
        bigquery.SchemaField("recommended_dte", "INTEGER"),
        bigquery.SchemaField("recommended_mid_price", "FLOAT"),
        bigquery.SchemaField("recommended_spread_pct", "FLOAT"),
        bigquery.SchemaField("contract_score", "FLOAT"),
        bigquery.SchemaField("recommended_delta", "FLOAT"),
        bigquery.SchemaField("recommended_gamma", "FLOAT"),
        bigquery.SchemaField("recommended_theta", "FLOAT"),
        bigquery.SchemaField("recommended_vega", "FLOAT"),
        bigquery.SchemaField("recommended_iv", "FLOAT"),
        bigquery.SchemaField("recommended_volume", "INTEGER"),
        bigquery.SchemaField("recommended_oi", "INTEGER"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="scan_date"
    )
    table.clustering_fields = ["overnight_score", "ticker"]
    try:
        bq.create_table(table, exists_ok=True)
        logger.info("Table %s ready.", table_id)
    except Exception as e:
        logger.error("Failed to create table %s: %s", table_id, e)
        raise


def _write_results(bq: bigquery.Client, scored: list[dict]):
    """Write scored results to BigQuery."""
    if not scored:
        logger.info("No results to write.")
        return

    now = datetime.now(timezone.utc)
    today = now.date()

    rows = []
    for s in scored:
        exp = s.get("recommended_expiration")
        if exp and isinstance(exp, str):
            try:
                exp = date.fromisoformat(exp).isoformat()
            except Exception:
                exp = None

        rows.append({
            "scan_date": today.isoformat(),
            "scan_timestamp": now.isoformat(),
            "ticker": s["ticker"],
            "direction": s.get("direction"),
            "overnight_score": s.get("overnight_score"),
            "price_change_pct": s.get("price_change_pct"),
            "underlying_price": s.get("underlying_price"),
            "day_volume": s.get("day_volume"),
            "call_dollar_volume": s.get("call_dollar_volume"),
            "put_dollar_volume": s.get("put_dollar_volume"),
            "total_options_dollar_volume": s.get("total_options_dollar_volume"),
            "call_vol_oi_ratio": s.get("call_vol_oi_ratio"),
            "put_vol_oi_ratio": s.get("put_vol_oi_ratio"),
            "call_active_strikes": s.get("call_active_strikes"),
            "put_active_strikes": s.get("put_active_strikes"),
            "call_uoa_depth": s.get("call_uoa_depth"),
            "put_uoa_depth": s.get("put_uoa_depth"),
            "signals": s.get("signals", []),
            "recommended_contract": s.get("recommended_contract"),
            "recommended_strike": s.get("recommended_strike"),
            "recommended_expiration": exp,
            "recommended_dte": s.get("recommended_dte"),
            "recommended_mid_price": s.get("recommended_mid_price"),
            "recommended_spread_pct": s.get("recommended_spread_pct"),
            "contract_score": s.get("contract_score"),
            "recommended_delta": s.get("recommended_delta"),
            "recommended_gamma": s.get("recommended_gamma"),
            "recommended_theta": s.get("recommended_theta"),
            "recommended_vega": s.get("recommended_vega"),
            "recommended_iv": s.get("recommended_iv"),
            "recommended_volume": s.get("recommended_volume"),
            "recommended_oi": s.get("recommended_oi"),
            "inserted_at": now.isoformat(),
        })

    errors = bq.insert_rows_json(config.OVERNIGHT_SIGNALS_TABLE, rows)
    if errors:
        logger.error("BigQuery insert errors: %s", errors)
    else:
        logger.info("Wrote %d rows to %s", len(rows), config.OVERNIGHT_SIGNALS_TABLE)


# =====================================================================
# MAIN PIPELINE
# =====================================================================

def run_pipeline():
    """Main entry point for the overnight scanner."""
    logger.info("=" * 60)
    logger.info("OVERNIGHT FLOW SCANNER v1 — Starting")
    logger.info("=" * 60)

    bq = bigquery.Client(project=config.PROJECT_ID)
    poly = PolygonClient(api_key=config.POLYGON_API_KEY)

    # Check idempotency — skip if already ran today
    today_str = date.today().isoformat()
    check_q = f"""
        SELECT COUNT(*) as cnt FROM `{config.OVERNIGHT_SIGNALS_TABLE}`
        WHERE scan_date = '{today_str}'
    """
    try:
        result = list(bq.query(check_q))
        if result and result[0]["cnt"] > 0:
            logger.info("Already scanned for %s. Skipping.", today_str)
            return
    except Exception:
        pass  # Table might not exist yet

    # Ensure output table exists
    _ensure_table(bq)

    # Step 1: Load universe
    universe = _load_universe()
    if not universe:
        logger.error("Empty universe. Aborting.")
        return

    # Step 2: Pass 1 — stock snapshots, filter movers
    movers = _pass1_stock_snapshots(poly, universe)
    if not movers:
        logger.info("No movers found. Nothing to scan.")
        return

    # Step 3: Pass 2 — options chains for movers
    enriched = _pass2_options(poly, movers)
    if not enriched:
        logger.info("No options data collected. Exiting.")
        return

    # Step 4: Score everything
    scored = [_score_ticker(d) for d in enriched]
    scored.sort(key=lambda x: x["overnight_score"], reverse=True)

    # Step 5: Filter to min score and cap at 10
    top = [s for s in scored if s["overnight_score"] >= MIN_SCORE][:10]

    logger.info("=" * 60)
    logger.info("RESULTS: %d tickers scored >= %d (out of %d total)", len(top), MIN_SCORE, len(scored))
    for t in top:
        contract = t.get("recommended_contract") or "NO CONTRACT"
        logger.info(
            "  %s | Score %d | %s | %s | $%.2f | %s",
            t["ticker"],
            t["overnight_score"],
            t["direction"],
            contract,
            t.get("recommended_mid_price") or 0,
            " | ".join(t.get("signals", [])),
        )
    logger.info("=" * 60)

    # Step 6: Write ALL scored tickers (not just top 10) for analysis
    # But mark which ones made the cut
    _write_results(bq, scored)

    logger.info("Overnight scanner complete. %d signals surfaced.", len(top))
    return top
