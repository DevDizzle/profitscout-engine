import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from google.cloud import bigquery

from .. import config as enrichment_config
from ..clients.polygon_client import PolygonClient

# Configure Logging
logger = logging.getLogger(__name__)


def run_pipeline():
    logger.info("Starting Momentum Scanner pipeline...")

    client = bigquery.Client(project=enrichment_config.PROJECT_ID)
    # Ensure we use the API key from enrichment config
    polygon = PolygonClient(api_key=enrichment_config.POLYGON_API_KEY)

    # Step 1: Load Morning Watchlist from Winners Dashboard
    # We select tickers/contracts that made it to the 'Winners' list.
    # We JOIN back to options_candidates to get the quantitative baseline (Snapshot A)
    # because the dashboard table doesn't store Greeks/Pricing details.
    query = f"""
    WITH LatestDashboard AS (
        SELECT *
        FROM `{enrichment_config.PROJECT_ID}.{enrichment_config.BIGQUERY_DATASET}.winners_dashboard`
        WHERE run_date = (
            SELECT MAX(run_date) 
            FROM `{enrichment_config.PROJECT_ID}.{enrichment_config.BIGQUERY_DATASET}.winners_dashboard`
        )
    ),
    LatestCandidates AS (
        SELECT *
        FROM `{enrichment_config.PROJECT_ID}.{enrichment_config.BIGQUERY_DATASET}.options_candidates`
        WHERE selection_run_ts = (
            SELECT MAX(selection_run_ts) 
            FROM `{enrichment_config.PROJECT_ID}.{enrichment_config.BIGQUERY_DATASET}.options_candidates`
        )
    )
    SELECT 
        wd.ticker,
        wd.contract_symbol,
        wd.option_type,
        wd.strike_price AS strike,
        CAST(wd.expiration_date AS DATE) AS expiration_date,
        
        -- Snapshot A Metrics from Candidates Table
        c.last_price AS snapshot_a_price,
        c.bid AS snapshot_a_bid,
        c.ask AS snapshot_a_ask,
        c.volume AS snapshot_a_volume,
        c.open_interest AS snapshot_a_oi,
        c.implied_volatility AS snapshot_a_iv,
        c.underlying_price AS snapshot_a_underlying,
        
        -- Scores & Signals
        wd.options_score,
        CASE 
            WHEN wd.option_type = 'call' THEN 'BUY' 
            ELSE 'SELL' 
        END AS signal,
        wd.setup_quality_signal,
        wd.volatility_comparison_signal
        
    FROM LatestDashboard wd
    JOIN LatestCandidates c ON wd.contract_symbol = c.contract_symbol
    -- No further filtering needed as Dashboard already filters for quality
    ORDER BY wd.options_score DESC
    LIMIT 50
    """

    logger.info("Fetching morning watchlist from BigQuery...")
    query_job = client.query(query)
    candidates = [dict(row) for row in query_job.result()]

    if not candidates:
        logger.warning("No candidates found in morning watchlist. Exiting.")
        return

    logger.info(f"Found {len(candidates)} candidates. Fetching real-time snapshots...")

    # Step 2: Re-fetch Current Data from Polygon
    def fetch_live_data(cand):
        ticker = cand["ticker"]
        contract = cand["contract_symbol"]

        # Fetch underlying snapshot
        stock_snap = polygon.fetch_stock_snapshot(ticker)
        # Fetch option snapshot
        opt_snap = polygon.fetch_option_contract_snapshot(ticker, contract)

        return {"candidate": cand, "stock_snap": stock_snap, "opt_snap": opt_snap}

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_live_data, c): c for c in candidates}
        for future in as_completed(futures):
            try:
                res = future.result()
                if res["stock_snap"] and res["opt_snap"]:
                    results.append(res)
                else:
                    logger.warning(
                        f"Missing snapshot data for {res['candidate']['contract_symbol']}"
                    )
            except Exception as e:
                logger.error(f"Error fetching data: {e}")

    # Step 3: Compare Snapshots and Score Momentum
    rows_to_insert = []
    scan_ts = datetime.now(timezone.utc)

    for item in results:
        cand = item["candidate"]
        stock_snap = item["stock_snap"]
        opt_snap = item["opt_snap"]

        # Parse Current Data
        # Stock Price
        t_data = stock_snap.get("ticker", {})
        current_underlying = t_data.get("lastTrade", {}).get("p")
        if not current_underlying:
            current_underlying = t_data.get("day", {}).get("c")  # Close if no last trade
        if not current_underlying:
            # Fallback to lastQuote mid
            lq = t_data.get("lastQuote", {})
            bp, ap = lq.get("bp"), lq.get("ap")
            if isinstance(bp, (int, float)) and isinstance(ap, (int, float)):
                current_underlying = (bp + ap) / 2

        if not current_underlying:
            logger.warning(
                f"Could not determine current underlying price for {cand['ticker']}"
            )
            continue

        current_underlying = float(current_underlying)

        # Option Data
        current_bid = opt_snap.get("bid")
        current_ask = opt_snap.get("ask")
        current_volume = opt_snap.get("volume") or 0
        current_iv = opt_snap.get("implied_volatility")

        if current_bid is None or current_ask is None:
            logger.warning(f"Missing bid/ask for {cand['contract_symbol']}")
            continue

        # Comparisons
        snapshot_a_mid = (cand["snapshot_a_bid"] + cand["snapshot_a_ask"]) / 2
        snapshot_b_mid = (current_bid + current_ask) / 2

        # Handle zero division if morning price was 0
        if snapshot_a_mid > 0:
            premium_change_pct = (
                (snapshot_b_mid - snapshot_a_mid) / snapshot_a_mid * 100
            )
        else:
            premium_change_pct = 0

        snapshot_a_volume = cand["snapshot_a_volume"] or 0
        volume_ratio = current_volume / max(snapshot_a_volume, 1)

        snapshot_a_underlying = cand["snapshot_a_underlying"]
        underlying_change_pct = (
            (current_underlying - snapshot_a_underlying) / snapshot_a_underlying * 100
        )

        direction_confirmed = (
            cand["signal"] == "BUY" and underlying_change_pct > 0.2
        ) or (cand["signal"] == "SELL" and underlying_change_pct < -0.2)

        # Momentum Score
        momentum_score = 0
        if premium_change_pct > 5:
            momentum_score += 3
        elif premium_change_pct > 2:
            momentum_score += 2
        elif premium_change_pct > 0:
            momentum_score += 1

        if volume_ratio >= 3:
            momentum_score += 3
        elif volume_ratio >= 2:
            momentum_score += 2
        elif volume_ratio >= 1.5:
            momentum_score += 1

        if direction_confirmed:
            momentum_score += 2

        # IV Check
        snapshot_a_iv = cand["snapshot_a_iv"] or 0
        current_iv_val = current_iv or 0
        iv_change = current_iv_val - snapshot_a_iv
        if iv_change > 0.15:
            momentum_score -= 2

        # Signal Thresholds
        if momentum_score >= 6:
            momentum_signal = "STRONG"
        elif momentum_score >= 4:
            momentum_signal = "MODERATE"
        else:
            momentum_signal = "NO_ENTRY"

        row = {
            "scan_ts": scan_ts.isoformat(),
            "ticker": cand["ticker"],
            "contract_symbol": cand["contract_symbol"],
            "option_type": cand["option_type"],
            "signal": cand["signal"],
            "strike": cand["strike"],
            "expiration_date": cand["expiration_date"].isoformat()
            if hasattr(cand["expiration_date"], "isoformat")
            else str(cand["expiration_date"]),
            "snapshot_a_mid": float(snapshot_a_mid),
            "snapshot_b_mid": float(snapshot_b_mid),
            "premium_change_pct": float(premium_change_pct),
            "snapshot_a_volume": int(snapshot_a_volume),
            "current_volume": int(current_volume),
            "volume_ratio": float(volume_ratio),
            "snapshot_a_underlying": float(snapshot_a_underlying),
            "current_underlying": float(current_underlying),
            "underlying_change_pct": float(underlying_change_pct),
            "direction_confirmed": bool(direction_confirmed),
            "iv_change": float(iv_change),
            "momentum_score": int(momentum_score),
            "momentum_signal": momentum_signal,
            "options_score": cand["options_score"],
            "setup_quality": cand["setup_quality_signal"],
        }
        rows_to_insert.append(row)

    # Step 4: Write Results
    if rows_to_insert:
        table_id = f"{enrichment_config.PROJECT_ID}.{enrichment_config.BIGQUERY_DATASET}.momentum_signals"

        schema = [
            bigquery.SchemaField("scan_ts", "TIMESTAMP"),
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("contract_symbol", "STRING"),
            bigquery.SchemaField("option_type", "STRING"),
            bigquery.SchemaField("signal", "STRING"),
            bigquery.SchemaField("strike", "FLOAT"),
            bigquery.SchemaField("expiration_date", "DATE"),
            bigquery.SchemaField("snapshot_a_mid", "FLOAT"),
            bigquery.SchemaField("snapshot_b_mid", "FLOAT"),
            bigquery.SchemaField("premium_change_pct", "FLOAT"),
            bigquery.SchemaField("snapshot_a_volume", "INTEGER"),
            bigquery.SchemaField("current_volume", "INTEGER"),
            bigquery.SchemaField("volume_ratio", "FLOAT"),
            bigquery.SchemaField("snapshot_a_underlying", "FLOAT"),
            bigquery.SchemaField("current_underlying", "FLOAT"),
            bigquery.SchemaField("underlying_change_pct", "FLOAT"),
            bigquery.SchemaField("direction_confirmed", "BOOLEAN"),
            bigquery.SchemaField("iv_change", "FLOAT"),
            bigquery.SchemaField("momentum_score", "INTEGER"),
            bigquery.SchemaField("momentum_signal", "STRING"),
            bigquery.SchemaField("options_score", "FLOAT"),
            bigquery.SchemaField("setup_quality", "STRING"),
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="scan_ts",
            ),
        )

        try:
            client.load_table_from_json(
                rows_to_insert, table_id, job_config=job_config
            ).result()
            logger.info(f"Inserted {len(rows_to_insert)} rows into {table_id}")
        except Exception as e:
            logger.error(f"Failed to write to BigQuery: {e}")

    # Step 5: Return top results
    top_picks = sorted(
        rows_to_insert, key=lambda x: x["momentum_score"], reverse=True
    )[:5]
    logger.info(f"Top 5 Momentum Picks: {top_picks}")

    return top_picks
