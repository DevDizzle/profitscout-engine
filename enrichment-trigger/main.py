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

from flask import Flask, Request, jsonify, request
from google.cloud import bigquery, storage
from google import genai
from google.genai import types

app = Flask(__name__)

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
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "global")

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
           recommended_delta, recommended_gamma, recommended_theta, recommended_vega,
           recommended_iv, recommended_volume, recommended_oi, recommended_dte,
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

def fetch_and_analyze_news(ticker: str, direction: str, price_change_pct: float, flow_volume: float = 0) -> dict | None:
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

        prompt = f"""You are a senior institutional options flow analyst. Search for the latest news about {ticker} stock from the past 48 hours.

CONTEXT:
- Stock moved {price_change_pct:+.1f}% recently
- Institutional options flow direction: {direction}
- Flow volume: ${flow_volume:,.0f}

CRITICAL ANALYSIS: You must assess whether this options flow is DIRECTIONAL (a new bet on future movement) or HEDGING (protecting existing positions after a move already happened). This distinction is everything.

Key signals of HEDGING flow (not tradeable):
- Large flow AFTER a big move (>10%) in the same direction
- Flow is protecting existing equity positions
- The catalyst is already known/priced in

Key signals of DIRECTIONAL flow (tradeable):
- Flow appears BEFORE or independent of a catalyst
- Flow size is disproportionate to the move
- New information not yet reflected in price

Based on what you find, respond in valid JSON only (no markdown, no code fences):
{{
  "catalyst_score": <float 0.0-1.0, how strong is the news catalyst driving this move>,
  "catalyst_type": "<category: Earnings Beat, Earnings Miss, Guidance Raise, Guidance Cut, Analyst Upgrade, Analyst Downgrade, Sector Rotation, M&A, Regulatory, Product Launch, Partnership, Macro, Technical Breakout, Short Squeeze, Insider Activity, No Clear Catalyst>",
  "summary": "<2-3 sentence analysis of what is driving this move and whether institutional flow is likely to continue>",
  "key_headline": "<single most important headline you found>",
  "news_found": <boolean, true if you found relevant recent news>,
  "sources_count": <integer, number of distinct news sources found>,
  "flow_intent": "<DIRECTIONAL|HEDGING|MECHANICAL|MIXED — classify the likely intent of the institutional flow>",
  "flow_intent_reasoning": "<1 sentence explaining why you classified the flow this way>",
  "move_overdone": <boolean, true if the price move appears disproportionate to the catalyst>,
  "reversal_probability": <float 0.0-1.0, probability of a reversal in the next 1-5 trading days based on historical patterns for this type of event>,
  "thesis": "<2-3 sentence trade thesis synthesizing flow direction, catalyst, and setup. Write as a trader briefing: what's the trade, why now, what's the risk. Example: 'FSLY BULL $25C Mar 21. Agentic AI traffic driving blockbuster earnings beat with 1,986% call volume surge. Entry above $22 support with $28 resistance target. Risk: post-earnings fade if guidance disappoints on follow-through.'"
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
            # REMOVED: response_mime_type="application/json" (causes issues with grounding)
        )

        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=prompt,
            config=cfg,
        )

        text = response.text.strip()
        logger.info(f"  {ticker}: Grounded search response length={len(text)}")

        # Parse JSON manually (robust extraction)
        def _extract_json_object(text: str) -> str:
            if not text:
                return ""
            # Strip code fences
            import re
            text = re.sub(r"^\s*```json\s*", "", text, flags=re.MULTILINE)
            text = re.sub(r"```\s*$", "", text, flags=re.MULTILINE)
            text = text.strip()
            # Find the JSON bracket boundaries
            start = text.find("{")
            end = text.rfind("}")
            if start != -1 and end != -1 and end > start:
                return text[start : end + 1]
            return text

        clean_json = _extract_json_object(text)
        if not clean_json:
            logger.warning(f"  {ticker}: No JSON object found in response")
            return None

        # Try parsing as JSON
        result = json.loads(clean_json)

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
        result.setdefault("flow_intent", "MIXED")
        result.setdefault("flow_intent_reasoning", "Unable to determine flow intent.")
        result.setdefault("move_overdone", False)
        result.setdefault("thesis", "")
        try:
            result["reversal_probability"] = float(result.get("reversal_probability", 0.3))
        except (ValueError, TypeError):
            result["reversal_probability"] = 0.3

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
        # Pass the relevant flow volume for intent analysis
        if direction == "BULLISH":
            flow_vol = float(signal.get("call_dollar_volume", 0) or 0)
        else:
            flow_vol = float(signal.get("put_dollar_volume", 0) or 0)

        analysis = fetch_and_analyze_news(ticker, direction, move_pct, flow_vol)

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
        # Get 300 days of daily bars (need 200+ for SMA_200)
        from datetime import timedelta
        end_date = date.today().isoformat()
        start_date = (date.today() - timedelta(days=420)).isoformat()

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

        # --- Support / Resistance from price structure ---
        close_price = float(latest.get("close", 0) or 0)
        
        # 52-week high/low (from available data, up to 90 days)
        high_52w = safe_float(df["high"].max())
        low_52w = safe_float(df["low"].min())
        
        # Swing high/low detection (last 20 bars for near-term levels)
        recent = df.tail(20)
        recent_high = safe_float(recent["high"].max())
        recent_low = safe_float(recent["low"].min())
        
        # Support = strongest floor: max of (recent swing low, SMA 200, Bollinger lower)
        support_candidates = [v for v in [
            safe_float(recent_low),
            safe_float(latest.get("SMA_200")),
            safe_float(latest.get("BBL_20_2.0")),
        ] if v is not None and v < close_price]
        support = max(support_candidates) if support_candidates else safe_float(recent_low)
        
        # Resistance = strongest ceiling: min of (recent swing high, SMA 50 if above, Bollinger upper)
        resistance_candidates = [v for v in [
            safe_float(recent_high),
            safe_float(latest.get("SMA_50")),
            safe_float(latest.get("BBU_20_2.0")),
        ] if v is not None and v > close_price]
        resistance = min(resistance_candidates) if resistance_candidates else safe_float(recent_high)

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
            # NEW: Key levels
            "support": support,
            "resistance": resistance,
            "high_52w": high_52w,
            "low_52w": low_52w,
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
# STEP 4: Compute derived risk fields
# =====================================================================

def compute_risk_fields(sig: dict, tech: dict, news: dict) -> dict:
    """
    Compute mean_reversion_risk, atr_normalized_move, flow_intent,
    and enrichment_quality_score from raw signal + enrichment data.
    """
    import math

    pct = float(sig.get("price_change_pct", 0) or 0)
    direction = sig.get("direction", "")
    rsi = float(tech.get("rsi_14", 50) or 50)
    atr = float(tech.get("atr_14", 0) or 0)
    price = float(sig.get("underlying_price", 0) or 0)
    catalyst_score = float(news.get("catalyst_score", 0.1) or 0.1)
    reversal_prob = float(news.get("reversal_probability", 0.3) or 0.3)
    overnight_score = int(sig.get("overnight_score", 5) or 5)

    # --- ATR-normalized move ---
    # How many ATRs did the stock move? >2 = extreme, >1.5 = significant
    atr_pct = (atr / price * 100) if price > 0 and atr > 0 else 3.0  # default 3% daily range
    atr_normalized_move = abs(pct) / atr_pct if atr_pct > 0 else 0
    atr_normalized_move = round(atr_normalized_move, 2)

    # --- Mean-reversion risk (0.0-1.0) ---
    mr_risk = 0.0

    # Price already moved significantly in flow direction?
    flow_aligned = (direction == "BEARISH" and pct < 0) or (direction == "BULLISH" and pct > 0)
    if flow_aligned:
        if abs(pct) > 15:
            mr_risk += 0.45
        elif abs(pct) > 10:
            mr_risk += 0.30
        elif abs(pct) > 5:
            mr_risk += 0.10

    # RSI extremes
    if direction == "BEARISH" and rsi < 30:
        mr_risk += 0.25  # Oversold + bear flow = capitulation risk
    elif direction == "BEARISH" and rsi < 35:
        mr_risk += 0.15
    elif direction == "BULLISH" and rsi > 70:
        mr_risk += 0.25  # Overbought + bull flow = euphoria risk
    elif direction == "BULLISH" and rsi > 65:
        mr_risk += 0.15

    # ATR-normalized extremes
    if atr_normalized_move > 2.5:
        mr_risk += 0.20
    elif atr_normalized_move > 1.5:
        mr_risk += 0.10

    # Strong catalyst reduces reversion risk
    if catalyst_score > 0.8:
        mr_risk -= 0.10
    elif catalyst_score > 0.6:
        mr_risk -= 0.05

    # Gemini's reversal probability factors in
    mr_risk = mr_risk * 0.6 + reversal_prob * 0.4

    mr_risk = round(max(0.0, min(1.0, mr_risk)), 3)

    # --- Flow intent (from Gemini, with computed fallback) ---
    flow_intent = news.get("flow_intent", "MIXED")
    flow_intent_reasoning = news.get("flow_intent_reasoning", "")

    # --- Enrichment quality score (0-10) ---
    # Composite: overnight_score (40%) + catalyst relevance (20%) + 
    # inverse mean-reversion risk (20%) + technical alignment (20%)
    tech_alignment = 0.5  # neutral default
    if direction == "BULLISH":
        if rsi > 40 and rsi < 70:
            tech_alignment = 0.7  # healthy momentum zone
        elif rsi < 40:
            tech_alignment = 0.3  # buying into weakness
    elif direction == "BEARISH":
        if rsi < 60 and rsi > 30:
            tech_alignment = 0.7
        elif rsi > 60:
            tech_alignment = 0.3

    quality = (
        (overnight_score / 10) * 0.4 +
        catalyst_score * 0.2 +
        (1.0 - mr_risk) * 0.2 +
        tech_alignment * 0.2
    ) * 10
    quality = round(max(0, min(10, quality)), 1)

    return {
        "mean_reversion_risk": mr_risk,
        "atr_normalized_move": atr_normalized_move,
        "flow_intent": flow_intent,
        "flow_intent_reasoning": flow_intent_reasoning,
        "move_overdone": news.get("move_overdone", False),
        "reversal_probability": round(reversal_prob, 3),
        "enrichment_quality_score": quality,
    }


def _calc_risk_reward(price, direction, support, resistance):
    """Calculate risk/reward ratio from price and key levels."""
    try:
        price = float(price or 0)
        support = float(support or 0)
        resistance = float(resistance or 0)
        if price <= 0 or support <= 0 or resistance <= 0:
            return None
        if direction == "BULLISH":
            reward = resistance - price
            risk = price - support
        else:  # BEARISH
            reward = price - support
            risk = resistance - price
        if risk <= 0:
            return None
        return round(reward / risk, 2)
    except (ValueError, TypeError):
        return None


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
        
        # Compute derived risk fields
        risk = compute_risk_fields(sig, tech, news)
        
        if news:
            logger.info(f"  {ticker}: catalyst={news.get('catalyst_score')} intent={risk['flow_intent']} mr_risk={risk['mean_reversion_risk']} quality={risk['enrichment_quality_score']}")

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
            "recommended_dte": sig.get("recommended_dte"),
            "recommended_mid_price": sig.get("recommended_mid_price"),
            "recommended_spread_pct": sig.get("recommended_spread_pct"),
            "contract_score": sig.get("contract_score"),
            "recommended_delta": sig.get("recommended_delta"),
            "recommended_gamma": sig.get("recommended_gamma"),
            "recommended_theta": sig.get("recommended_theta"),
            "recommended_vega": sig.get("recommended_vega"),
            "recommended_iv": sig.get("recommended_iv"),
            "recommended_volume": sig.get("recommended_volume"),
            "recommended_oi": sig.get("recommended_oi"),
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
            # NEW: Flow intent analysis
            "flow_intent": risk["flow_intent"],
            "flow_intent_reasoning": risk["flow_intent_reasoning"],
            # NEW: Risk assessment
            "mean_reversion_risk": risk["mean_reversion_risk"],
            "atr_normalized_move": risk["atr_normalized_move"],
            "move_overdone": risk["move_overdone"],
            "reversal_probability": risk["reversal_probability"],
            "enrichment_quality_score": risk["enrichment_quality_score"],
            # NEW: AI Trade Thesis
            "thesis": news.get("thesis", ""),
            # NEW: Key Levels
            "support": tech.get("support"),
            "resistance": tech.get("resistance"),
            "high_52w": tech.get("high_52w"),
            "low_52w": tech.get("low_52w"),
            # NEW: Risk/Reward
            "risk_reward_ratio": _calc_risk_reward(
                sig.get("underlying_price", 0), sig.get("direction", ""),
                tech.get("support"), tech.get("resistance")
            ),
            # Metadata
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        }
        rows.append(row)

    if not rows:
        logger.warning("No enriched rows to write")
        return

    # Write to BigQuery — delete existing rows for this scan_date first (dedup)
    delete_query = f"DELETE FROM `{ENRICHED_SIGNALS_TABLE}` WHERE scan_date = '{scan_date}'"
    bq_client.query(delete_query).result()
    logger.info(f"Deleted existing rows for {scan_date} (dedup)")

    table_ref = bq_client.dataset(DATASET).table("overnight_signals_enriched")

    # Force numeric fields to proper types before writing
    for row in rows:
        for float_field in ["catalyst_score", "ema_21", "rsi_14", "macd", "macd_hist", "sma_50", "sma_200", "atr_14",
                            "call_vol_oi_ratio", "put_vol_oi_ratio", "recommended_mid_price", "recommended_spread_pct",
                            "contract_score", "price_change_pct", "underlying_price",
                            "recommended_delta", "recommended_gamma", "recommended_theta",
                            "recommended_vega", "recommended_iv", "recommended_strike"]:
            if row.get(float_field) is not None:
                try:
                    row[float_field] = float(row[float_field])
                except (ValueError, TypeError):
                    row[float_field] = None

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
            # Contract + Greeks
            "recommended_contract": sig.get("recommended_contract"),
            "recommended_strike": sig.get("recommended_strike"),
            "recommended_expiration": str(sig.get("recommended_expiration")) if sig.get("recommended_expiration") else None,
            "recommended_dte": sig.get("recommended_dte"),
            "recommended_mid_price": sig.get("recommended_mid_price"),
            "recommended_spread_pct": sig.get("recommended_spread_pct"),
            "contract_score": sig.get("contract_score"),
            "recommended_delta": sig.get("recommended_delta"),
            "recommended_gamma": sig.get("recommended_gamma"),
            "recommended_theta": sig.get("recommended_theta"),
            "recommended_vega": sig.get("recommended_vega"),
            "recommended_iv": sig.get("recommended_iv"),
            "recommended_volume": sig.get("recommended_volume"),
            "recommended_oi": sig.get("recommended_oi"),
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
            # Flow intent & risk
            "flow_intent": news.get("flow_intent", "MIXED"),
            "flow_intent_reasoning": news.get("flow_intent_reasoning", ""),
            "mean_reversion_risk": compute_risk_fields(sig, tech, news).get("mean_reversion_risk", 0),
            "atr_normalized_move": compute_risk_fields(sig, tech, news).get("atr_normalized_move", 0),
            "reversal_probability": news.get("reversal_probability", 0.3),
            "enrichment_quality_score": compute_risk_fields(sig, tech, news).get("enrichment_quality_score", 5),
            # NEW: AI Trade Thesis
            "thesis": news.get("thesis", ""),
            # NEW: Key Levels
            "support": tech.get("support"),
            "resistance": tech.get("resistance"),
            "high_52w": tech.get("high_52w"),
            "low_52w": tech.get("low_52w"),
            # NEW: Risk/Reward
            "risk_reward_ratio": _calc_risk_reward(
                sig.get("underlying_price", 0), sig.get("direction", ""),
                tech.get("support"), tech.get("resistance")
            ),
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
        "created_at": firestore.SERVER_TIMESTAMP,
        "updated_at": firestore.SERVER_TIMESTAMP,
    })

    batch.commit()
    logger.info(f"Synced {count} signals + summary to Firestore for {scan_date}")


# =====================================================================
# CLOUD FUNCTION ENTRY POINT
# =====================================================================

@app.route("/", methods=["GET", "POST"])
def enrichment_trigger():
    """
    Main entry point. Call after overnight scanner completes.
    Enriches high-score signals with technicals + news + AI analysis,
    writes to BigQuery + Firestore.
    """
    logger.info("=" * 60)
    logger.info("OVERNIGHT EDGE ENRICHMENT TRIGGER (Fixed grounding)")
    logger.info("Using google-genai>=0.3.0")
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
        return jsonify({"status": "no_signals", "scan_date": scan_date}), 200

    # Check for force flag (POST JSON body or query param)
    force = False
    if request.method == "POST" and request.is_json:
        force = request.get_json(silent=True).get("force", False)
    elif request.args.get("force"):
        force = True

    if not force:
        # Guard: skip if scan_date is stale (>3 calendar days old — covers 3-day weekends)
        scan_dt = datetime.strptime(scan_date, "%Y-%m-%d").date()
        if (date.today() - scan_dt).days > 3:
            logger.info(f"Scan date {scan_date} is stale (>3 days old). Skipping.")
            return jsonify({"status": "skipped_stale", "scan_date": scan_date, "today": str(date.today())}), 200

        # Guard: skip if already enriched for this scan_date
        existing_q = f"SELECT COUNT(*) as cnt FROM `{ENRICHED_SIGNALS_TABLE}` WHERE scan_date = '{scan_date}'"
        existing = list(bq_client.query(existing_q).result())[0]["cnt"]
        if existing > 0:
            logger.info(f"Already {existing} enriched rows for {scan_date}. Skipping.")
            return jsonify({"status": "already_enriched", "scan_date": scan_date, "existing_rows": existing}), 200

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
    return jsonify(summary), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
