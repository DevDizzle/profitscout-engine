"""
Overnight Edge Enrichment Trigger
Cloud Function: enrichment_trigger (HTTP)
Project: profitscout-fida8

Reads today's overnight_signals where score >= 6,
then triggers news + technicals enrichment for those tickers only.
"""

import json
import logging
import os
from datetime import date, datetime, timezone

import functions_framework
from flask import Request
from google.cloud import bigquery, storage
from google import genai
from google.genai import types

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Config ---
PROJECT_ID = os.getenv("PROJECT_ID", "profitscout-fida8")
DATASET = os.getenv("DATASET", "profit_scout")
GCS_BUCKET = os.getenv("GCS_BUCKET", "profit-scout-data")
SIGNALS_TABLE = f"{PROJECT_ID}.{DATASET}.overnight_signals"
MIN_SCORE = int(os.getenv("MIN_ENRICHMENT_SCORE", "6"))

# Polygon
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()

# Vertex AI / Gemini
VERTEX_PROJECT = os.getenv("VERTEX_PROJECT", PROJECT_ID)
VERTEX_LOCATION = "global"

# Model Config
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-3-flash-preview")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.7"))
TOP_P = float(os.getenv("TOP_P", "0.95"))
TOP_K = int(os.getenv("TOP_K", "30"))
SEED = int(os.getenv("SEED", "42"))
CANDIDATE_COUNT = int(os.getenv("CANDIDATE_COUNT", "1"))
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "8192"))

# Output prefixes in GCS
NEWS_OUTPUT_PREFIX = "overnight-enrichment/news/"
TECHNICALS_OUTPUT_PREFIX = "overnight-enrichment/technicals/"
ENRICHED_SIGNALS_TABLE = f"{PROJECT_ID}.{DATASET}.overnight_signals_enriched"


# =====================================================================
# STEP 1: Get today's high-score tickers from overnight_signals
# =====================================================================

def get_signal_tickers(bq_client: bigquery.Client, scan_date: str = None) -> list[dict]:
    """Fetch tickers with score >= MIN_SCORE from today's overnight scan."""
    if not scan_date:
        # Get latest scan date
        q = f"SELECT MAX(scan_date) as latest FROM `{SIGNALS_TABLE}`"
        result = list(bq_client.query(q).result())
        scan_date = str(result[0]["latest"]) if result else str(date.today())

    query = f"""
    SELECT ticker, direction, overnight_score, price_change_pct, underlying_price,
           signals, recommended_contract, recommended_strike, recommended_expiration,
           recommended_mid_price, recommended_spread_pct, contract_score,
           call_dollar_volume, put_dollar_volume, call_uoa_depth, put_uoa_depth,
           call_active_strikes, put_active_strikes, call_vol_oi_ratio, put_vol_oi_ratio
    FROM `{SIGNALS_TABLE}`
    WHERE scan_date = '{scan_date}'
      AND overnight_score >= {MIN_SCORE}
    ORDER BY overnight_score DESC
    """
    rows = list(bq_client.query(query).result())
    logger.info(f"Found {len(rows)} signals with score >= {MIN_SCORE} for {scan_date}")
    return [dict(r) for r in rows], scan_date


# =====================================================================
# STEP 2: Fetch & Analyze news (Gemini Grounded Search)
# =====================================================================

def fetch_and_analyze_news(ticker: str, direction: str, price_change_pct: float) -> dict | None:
    """
    Use Gemini with Google Search grounding to fetch and analyze
    recent news for a ticker in a single call.
    """
    try:
        client = genai.Client(
            vertexai=True,
            project=VERTEX_PROJECT,
            location=VERTEX_LOCATION,
            http_options=types.HttpOptions(
                api_version="v1beta1",
                timeout=60000,
            ),
        )

        # Google Search grounding tool
        search_tool = types.Tool(google_search=types.GoogleSearch())

        prompt = f"""You are a financial analyst. Search for the latest news about {ticker} stock from the past 48 hours.

The stock moved {price_change_pct:+.1f}% and institutional options flow is {direction}.

Based on what you find, respond in valid JSON only (no markdown, no code fences):
{{
  "catalyst_score": <float 0.0-1.0, how strong is the news catalyst driving this move>,
  "catalyst_type": "<category: Earnings Beat, Earnings Miss, Guidance Raise, Guidance Cut, Analyst Upgrade, Analyst Downgrade, Sector Rotation, M&A, Regulatory, Product Launch, Partnership, Macro, Technical Breakout, Short Squeeze, Insider Activity, No Clear Catalyst>",
  "summary": "<2-3 sentence analysis of what is driving this move and whether institutional flow is likely to continue>",
  "key_headline": "<single most important headline you found>",
  "news_found": <boolean, true if you found relevant recent news>,
  "sources_count": <integer, number of distinct news sources found>
}}

If you find no relevant news, set catalyst_type to "No Clear Catalyst", catalyst_score to 0.1, and provide a summary noting the lack of news coverage."""

        cfg = types.GenerateContentConfig(
            temperature=TEMPERATURE,
            top_p=TOP_P,
            top_k=TOP_K,
            seed=SEED,
            candidate_count=CANDIDATE_COUNT,
            max_output_tokens=MAX_OUTPUT_TOKENS,
            tools=[search_tool],
            response_mime_type="application/json",
        )

        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=prompt,
            config=cfg,
        )

        text = response.text.strip()
        logger.info(f"  {ticker}: Grounded search response length={len(text)}")

        # Parse JSON — handle markdown code fences if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            text = text.rsplit("```", 1)[0]
        text = text.strip()

        # Try parsing as JSON
        result = json.loads(text)

        # Handle list response (Gemini sometimes returns a list)
        if isinstance(result, list) and len(result) > 0:
            result = result[0]

        # Validate required fields
        if not isinstance(result, dict):
            logger.warning(f"  {ticker}: Grounded search returned non-dict: {type(result)}")
            return None

        # Ensure catalyst_score is a float
        try:
            result["catalyst_score"] = float(result.get("catalyst_score", 0.1))
        except (ValueError, TypeError):
            result["catalyst_score"] = 0.1

        # Ensure required fields exist with defaults
        result.setdefault("catalyst_type", "No Clear Catalyst")
        result.setdefault("summary", f"No detailed analysis available for {ticker}.")
        result.setdefault("key_headline", f"{ticker} moves {price_change_pct:+.1f}%")
        result.setdefault("news_found", bool(result.get("sources_count", 0) > 0))
        result.setdefault("sources_count", 0)

        return result

    except json.JSONDecodeError as e:
        logger.error(f"  {ticker}: Failed to parse grounded search JSON: {e}")
        logger.error(f"  {ticker}: Raw text: {text[:500]}")
        return None
    except Exception as e:
        logger.error(f"  {ticker}: Grounded search failed: {e}")
        return None


def fetch_and_analyze_news_batch(
    signals: list[dict],
    gcs_client: storage.Client
) -> dict:
    """
    Fetch + analyze news for all tickers using Gemini grounded search.
    Stores results in GCS and returns analysis dict.
    """
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    bucket = gcs_client.bucket(GCS_BUCKET)
    results = {}
    today = date.today().isoformat()

    def _process_one(signal):
        ticker = signal["ticker"]
        direction = signal.get("direction", "unknown")
        move_pct = signal.get("price_change_pct", 0.0)

        analysis = fetch_and_analyze_news(ticker, direction, move_pct)

        # Store result in GCS for audit trail
        if analysis:
            blob_path = f"{NEWS_OUTPUT_PREFIX}{ticker}_{today}.json"
            blob = bucket.blob(blob_path)
            blob.upload_from_string(
                json.dumps(analysis, default=str),
                content_type="application/json"
            )

        return ticker, analysis

    # Gemini has higher rate limits than Polygon, but be respectful
    # Process 4 at a time with small delay
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_process_one, s): s["ticker"] for s in signals}
        for future in as_completed(futures):
            ticker, analysis = future.result()
            results[ticker] = analysis
            time.sleep(0.3)  # Rate limit: ~3 req/sec to be safe

    news_found = sum(1 for v in results.values() if v and v.get("news_found"))
    no_news = sum(1 for v in results.values() if v and not v.get("news_found"))
    failed = sum(1 for v in results.values() if v is None)

    logger.info(f"Grounded news: {news_found} with news, {no_news} no news, {failed} failed out of {len(signals)} tickers")

    return results


# =====================================================================
# STEP 3: Fetch technicals for each ticker (Polygon + pandas_ta)
# =====================================================================

def fetch_technicals_for_ticker(ticker: str, polygon_key: str) -> dict | None:
    """Fetch price history and compute technical indicators."""
    import requests

    try:
        # Get 60 days of daily bars
        from datetime import timedelta
        end_date = date.today().isoformat()
        start_date = (date.today() - timedelta(days=90)).isoformat()

        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
        params = {"adjusted": "true", "sort": "asc", "apiKey": polygon_key}

        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        bars = data.get("results", [])

        if len(bars) < 20:
            logger.warning(f"  {ticker}: only {len(bars)} bars, skipping technicals")
            return None

        # Compute indicators
        import pandas as pd
        import math

        df = pd.DataFrame(bars)
        df["date"] = pd.to_datetime(df["t"], unit="ms").dt.strftime("%Y-%m-%d")
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})

        # Core indicators
        try:
            import pandas_ta as ta
            df.ta.rsi(length=14, append=True)
            df.ta.macd(fast=12, slow=26, signal=9, append=True)
            df.ta.sma(length=50, append=True)
            df.ta.sma(length=200, append=True)
            df.ta.ema(length=21, append=True)
            df.ta.obv(append=True)
            df.ta.bbands(length=20, append=True)
            df.ta.atr(length=14, append=True)
        except ImportError:
            logger.warning("pandas_ta not available, computing basic indicators only")
            df["RSI_14"] = None
            df["SMA_50"] = df["close"].rolling(50).mean()
            df["SMA_200"] = df["close"].rolling(200).mean()
            df["EMA_21"] = df["close"].ewm(span=21).mean()

        # Get latest row
        latest = df.iloc[-1]

        def safe_float(x):
            try:
                v = float(x)
                return None if (math.isnan(v) or math.isinf(v)) else round(v, 4)
            except:
                return None

        result = {
            "ticker": ticker,
            "date": latest.get("date"),
            "close": safe_float(latest.get("close")),
            "volume": safe_float(latest.get("volume")),
            "rsi_14": safe_float(latest.get("RSI_14")),
            "macd": safe_float(latest.get("MACD_12_26_9")),
            "macd_hist": safe_float(latest.get("MACDh_12_26_9")),
            "macd_signal": safe_float(latest.get("MACDs_12_26_9")),
            "sma_50": safe_float(latest.get("SMA_50")),
            "sma_200": safe_float(latest.get("SMA_200")),
            "ema_21": safe_float(latest.get("EMA_21")),
            "obv": safe_float(latest.get("OBV")),
            "bband_upper": safe_float(latest.get("BBU_20_2.0")),
            "bband_lower": safe_float(latest.get("BBL_20_2.0")),
            "atr_14": safe_float(latest.get("ATRr_14")),
            # Derived signals
            "above_sma_50": bool(latest.get("close", 0) > latest.get("SMA_50", 0)) if latest.get("SMA_50") else None,
            "above_sma_200": bool(latest.get("close", 0) > latest.get("SMA_200", 0)) if latest.get("SMA_200") else None,
            "golden_cross": bool(latest.get("SMA_50", 0) > latest.get("SMA_200", 0)) if latest.get("SMA_200") else None,
        }

        logger.info(f"  {ticker}: technicals computed (RSI={result['rsi_14']}, SMA50={'above' if result['above_sma_50'] else 'below'})")
        return result

    except Exception as e:
        logger.error(f"  {ticker}: technicals failed: {e}")
        return None


def fetch_technicals_batch(tickers: list[str], polygon_key: str, gcs_client: storage.Client) -> dict:
    """Fetch technicals for all tickers and store in GCS."""
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    bucket = gcs_client.bucket(GCS_BUCKET)
    results = {}
    today = date.today().isoformat()

    def _fetch_one(ticker):
        tech = fetch_technicals_for_ticker(ticker, polygon_key)
        if tech:
            blob_path = f"{TECHNICALS_OUTPUT_PREFIX}{ticker}_{today}.json"
            blob = bucket.blob(blob_path)
            blob.upload_from_string(
                json.dumps(tech, default=str),
                content_type="application/json"
            )
        return ticker, tech

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_one, t): t for t in tickers}
        for future in as_completed(futures):
            ticker, tech = future.result()
            results[ticker] = tech
            time.sleep(0.15)

    logger.info(f"Technicals computed for {len([v for v in results.values() if v])} tickers")
    return results


# =====================================================================
# STEP 5: Write enriched signals to BigQuery
# =====================================================================

def write_enriched_signals(
    bq_client: bigquery.Client,
    signals: list[dict],
    technicals: dict,
    news_analysis: dict,
    scan_date: str
):
    """Merge signals + technicals + news into enriched table."""
    rows = []
    for sig in signals:
        ticker = sig["ticker"]
        tech = technicals.get(ticker, {}) or {}
        news = news_analysis.get(ticker, {}) or {}
        
        if news:
            logger.info(f"  {ticker}: merging news data — catalyst_score={news.get('catalyst_score')}")

        row = {
            "scan_date": scan_date,
            "ticker": ticker,
            "direction": sig["direction"],
            "overnight_score": sig["overnight_score"],
            "price_change_pct": sig["price_change_pct"],
            "underlying_price": sig["underlying_price"],
            # Flow data
            "call_dollar_volume": sig.get("call_dollar_volume"),
            "put_dollar_volume": sig.get("put_dollar_volume"),
            "call_uoa_depth": sig.get("call_uoa_depth"),
            "put_uoa_depth": sig.get("put_uoa_depth"),
            "call_active_strikes": sig.get("call_active_strikes"),
            "put_active_strikes": sig.get("put_active_strikes"),
            "call_vol_oi_ratio": sig.get("call_vol_oi_ratio"),
            "put_vol_oi_ratio": sig.get("put_vol_oi_ratio"),
            # Contract recommendation
            "recommended_contract": sig.get("recommended_contract"),
            "recommended_strike": sig.get("recommended_strike"),
            "recommended_expiration": str(sig.get("recommended_expiration")) if sig.get("recommended_expiration") else None,
            "recommended_mid_price": sig.get("recommended_mid_price"),
            "recommended_spread_pct": sig.get("recommended_spread_pct"),
            "contract_score": sig.get("contract_score"),
            # Technicals
            "rsi_14": tech.get("rsi_14"),
            "macd": tech.get("macd"),
            "macd_hist": tech.get("macd_hist"),
            "sma_50": tech.get("sma_50"),
            "sma_200": tech.get("sma_200"),
            "ema_21": float(tech.get("ema_21")) if tech.get("ema_21") is not None else None,
            "atr_14": tech.get("atr_14"),
            "above_sma_50": tech.get("above_sma_50"),
            "above_sma_200": tech.get("above_sma_200"),
            "golden_cross": tech.get("golden_cross"),
            # News analysis
            "catalyst_score": float(news.get("catalyst_score")) if news.get("catalyst_score") is not None else None,
            "catalyst_type": news.get("catalyst_type"),
            "news_summary": news.get("summary"),
            "key_headline": news.get("key_headline"),
            # Metadata
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        }
        rows.append(row)

    if not rows:
        logger.warning("No enriched rows to write")
        return

    # Write to BigQuery
    table_ref = bq_client.dataset(DATASET).table("overnight_signals_enriched")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    import io
    jsonl = "\n".join(json.dumps(r, default=str) for r in rows)
    job = bq_client.load_table_from_file(
        io.BytesIO(jsonl.encode("utf-8")),
        table_ref,
        job_config=job_config,
    )
    job.result()
    logger.info(f"Wrote {len(rows)} enriched signals to {ENRICHED_SIGNALS_TABLE}")


# =====================================================================
# STEP 6: Sync to Firestore (for webapp)
# =====================================================================

def sync_to_firestore(signals: list[dict], technicals: dict, news_analysis: dict, scan_date: str):
    """Sync enriched signals to Firestore for the webapp to read."""
    from google.cloud import firestore

    db = firestore.Client(project=PROJECT_ID)
    batch = db.batch()
    count = 0

    # Write each signal as a document
    for sig in signals:
        ticker = sig["ticker"]
        tech = technicals.get(ticker, {}) or {}
        news = news_analysis.get(ticker, {}) or {}

        doc_ref = db.collection("overnight_signals").document(f"{scan_date}_{ticker}")
        doc_data = {
            "scan_date": scan_date,
            "ticker": ticker,
            "direction": sig["direction"],
            "overnight_score": sig["overnight_score"],
            "price_change_pct": sig["price_change_pct"],
            "underlying_price": sig["underlying_price"],
            "signals": sig.get("signals", []),
            # Flow
            "call_dollar_volume": sig.get("call_dollar_volume"),
            "put_dollar_volume": sig.get("put_dollar_volume"),
            "call_uoa_depth": sig.get("call_uoa_depth"),
            "put_uoa_depth": sig.get("put_uoa_depth"),
            "call_active_strikes": sig.get("call_active_strikes"),
            "put_active_strikes": sig.get("put_active_strikes"),
            # Contract
            "recommended_contract": sig.get("recommended_contract"),
            "recommended_strike": sig.get("recommended_strike"),
            "recommended_expiration": str(sig.get("recommended_expiration")) if sig.get("recommended_expiration") else None,
            "recommended_mid_price": sig.get("recommended_mid_price"),
            "contract_score": sig.get("contract_score"),
            # Technicals
            "rsi_14": tech.get("rsi_14"),
            "macd_hist": tech.get("macd_hist"),
            "sma_50": tech.get("sma_50"),
            "sma_200": tech.get("sma_200"),
            "above_sma_50": tech.get("above_sma_50"),
            "above_sma_200": tech.get("above_sma_200"),
            "golden_cross": tech.get("golden_cross"),
            "atr_14": tech.get("atr_14"),
            # News
            "catalyst_score": news.get("catalyst_score"),
            "catalyst_type": news.get("catalyst_type"),
            "news_summary": news.get("summary"),
            "key_headline": news.get("key_headline"),
            "news_found": news.get("news_found", False),
            # Meta
            "enriched_at": datetime.now(timezone.utc),
            "updated_at": firestore.SERVER_TIMESTAMP,
        }

        batch.set(doc_ref, doc_data)
        count += 1

        if count % 400 == 0:
            batch.commit()
            batch = db.batch()

    # Write daily summary document
    summary_ref = db.collection("overnight_summaries").document(scan_date)
    bull_count = len([s for s in signals if s["direction"] == "BULLISH"])
    bear_count = len([s for s in signals if s["direction"] == "BEARISH"])

    batch.set(summary_ref, {
        "scan_date": scan_date,
        "total_signals": len(signals),
        "bullish_count": bull_count,
        "bearish_count": bear_count,
        "top_bullish": [s["ticker"] for s in signals if s["direction"] == "BULLISH"][:10],
        "top_bearish": [s["ticker"] for s in signals if s["direction"] == "BEARISH"][:10],
        "updated_at": firestore.SERVER_TIMESTAMP,
    })

    batch.commit()
    logger.info(f"Synced {count} signals + summary to Firestore for {scan_date}")


# =====================================================================
# CLOUD FUNCTION ENTRY POINT
# =====================================================================

@functions_framework.http
def enrichment_trigger(request: Request):
    """
    Main entry point. Call after overnight scanner completes.
    Enriches high-score signals with technicals + news + AI analysis,
    writes to BigQuery + Firestore.
    """
    logger.info("=" * 60)
    logger.info("OVERNIGHT EDGE ENRICHMENT TRIGGER")
    logger.info("=" * 60)

    # Init clients
    bq_client = bigquery.Client(project=PROJECT_ID)
    gcs_client = storage.Client(project=PROJECT_ID)

    if POLYGON_API_KEY:
        logger.info(f"POLYGON_API_KEY is set (length: {len(POLYGON_API_KEY)}).")
    else:
        logger.warning("POLYGON_API_KEY is NOT set.")

    # Step 1: Get today's high-score signals
    signals, scan_date = get_signal_tickers(bq_client)
    if not signals:
        return json.dumps({"status": "no_signals", "scan_date": scan_date}), 200

    tickers = list(set(s["ticker"] for s in signals))
    logger.info(f"Enriching {len(tickers)} tickers for {scan_date}")

    # Step 2: Fetch & Analyze News (Gemini Grounded Search)
    # Replaces the old two-step Polygon + Gemini process
    news_results = fetch_and_analyze_news_batch(signals, gcs_client)

    # Step 3: Fetch technicals
    technicals = {}
    if POLYGON_API_KEY:
        technicals = fetch_technicals_batch(tickers, POLYGON_API_KEY, gcs_client)
    else:
        logger.warning("No POLYGON_API_KEY — skipping technicals")

    # Step 5: Write enriched signals to BigQuery
    write_enriched_signals(bq_client, signals, technicals, news_results, scan_date)

    # Step 6: Sync to Firestore
    sync_to_firestore(signals, technicals, news_results, scan_date)

    # === OBSERVABILITY SUMMARY ===
    logger.info("=" * 60)
    logger.info("ENRICHMENT RUN SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Scan date: {scan_date}")
    logger.info(f"Signals enriched: {len(signals)}")
    logger.info(f"Unique tickers: {len(tickers)}")
    logger.info(f"")
    logger.info(f"--- TECHNICALS ---")
    tech_success = len([v for v in technicals.values() if v])
    tech_fail = len([v for v in technicals.values() if not v])
    logger.info(f"Success: {tech_success}, Failed/Empty: {tech_fail}")
    if technicals:
        sample = next((t for t in technicals.values() if t), None)
        if sample:
            logger.info(f"Sample: {sample.get('ticker')} RSI={sample.get('rsi_14')} SMA50={sample.get('sma_50')}")
    logger.info(f"")
    logger.info(f"--- NEWS (GROUNDED SEARCH) ---")
    news_found_count = sum(1 for v in news_results.values() if v and v.get('news_found'))
    no_news_count = sum(1 for v in news_results.values() if v and not v.get('news_found'))
    failed_count = sum(1 for v in news_results.values() if v is None)
    
    logger.info(f"News found: {news_found_count}")
    logger.info(f"No news: {no_news_count}")
    logger.info(f"Failed: {failed_count}")
    
    if news_results:
        sample_ticker = list(news_results.keys())[0]
        sample = news_results[sample_ticker]
        if sample:
            logger.info(f"Sample: {sample_ticker} catalyst={sample.get('catalyst_score')} type={sample.get('catalyst_type')}")
    logger.info(f"")
    logger.info(f"--- FIRESTORE ---")
    logger.info(f"Documents written: {len(signals)} signals + 1 summary")
    logger.info("=" * 60)

    summary = {
        "status": "success",
        "scan_date": scan_date,
        "signals_enriched": len(signals),
        "tickers": len(tickers),
        "news_found": news_found_count,
        "technicals_computed": len([v for v in technicals.values() if v]),
    }
    logger.info(f"ENRICHMENT COMPLETE: {json.dumps(summary)}")
    return json.dumps(summary), 200
