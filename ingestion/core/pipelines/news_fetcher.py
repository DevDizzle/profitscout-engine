# ingestion/core/pipelines/news_fetcher.py
import logging
import datetime
import os
import re
from typing import List, Dict, Iterable, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage, bigquery
from bs4 import BeautifulSoup
from .. import config, gcs
from ..clients.polygon_client import PolygonClient

# --- Configuration ---
NEWS_OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
STOCK_METADATA_TABLE_ID = config.MASTER_TABLE_ID # Use master table from config

# --- Tunables ---
TICKER_NEWS_LIMIT = int(os.getenv("NEWS_TICKER_LIMIT", "5"))
MACRO_GLOBAL_LIMIT = int(os.getenv("NEWS_MACRO_GLOBAL_LIMIT", "5"))
MACRO_SECTOR_LIMIT = int(os.getenv("NEWS_MACRO_SECTOR_LIMIT", "5"))
V2_PER_PAGE = int(os.getenv("NEWS_V2_PER_PAGE", "1000"))

# Minimum signal gates (lower = more throughput)
MIN_GLOBAL_SCORE = int(os.getenv("NEWS_MIN_GLOBAL_SCORE", "1"))
MIN_SECTOR_SCORE = int(os.getenv("NEWS_MIN_SECTOR_SCORE", "0"))

PUBLISHER_ALLOW = os.getenv(
    "NEWS_PUBLISHER_ALLOW",
    "Reuters,CNBC,MarketWatch,The Wall Street Journal,Financial Times,Bloomberg,Investing.com,Barron's,Yahoo Finance"
)
PUBLISHER_BLOCK = os.getenv(
    "NEWS_PUBLISHER_BLOCK",
    "GlobeNewswire,PR Newswire,Business Wire,Accesswire,Seeking Alpha PR"
)

TITLE_EXCLUDES = {
    "class action", "shareholder rights", "investors who lost money",
    "deadline alert", "lawsuit", "whale activity", "options activity",
    "press release"
}

# =========================
# Global macro lexicon (expanded)
# =========================
MACRO_KEYWORDS = {
    "cpi", "core cpi", "pce", "core pce", "ppi", "inflation", "deflation", "disinflation",
    "fomc", "federal reserve", "powell", "rate hike", "rate cut", "interest rate",
    "dot plot", "fed minutes", "qe", "qt", "jobs report", "nonfarm payroll", "nfp",
    "unemployment", "jolts", "gdp", "recession", "soft landing", "ism", "pmi",
    "treasury yield", "bond market", "curve", "inversion", "ecb", "boj", "boe", "pboc",
    "geopolitics", "tariff", "sanction"
}

# =========================
# Sector lexicon (sector-level; macro terms intentionally excluded)
# Keys must be slugified sector names, e.g. "financial-services"
# =========================
SECTOR_KEYWORDS = {
    "communication-services": {
        "ad spend", "cpm", "roas", "streaming subs", "content spend",
        "sports rights", "algorithm change", "engagement", "cord cutting",
        "arpu", "churn"
    },
    "consumer-cyclical": {
        "same-store sales", "comps", "traffic", "ticket size", "promotions",
        "inventory turnover", "order cancellations", "preorders",
        "e-commerce penetration", "loyalty members", "revpar", "adr"
    },
    "consumer-defensive": {
        "price/mix", "private label", "trade-down", "elasticities",
        "shelf resets", "input costs", "commodity hedges", "promo intensity",
        "distribution deals"
    },
    "energy": {
        "rig count", "frac spread", "day rates", "differentials",
        "crack spread", "refinery utilization", "takeaway capacity",
        "pipeline tariffs", "lng", "hedging", "turnarounds"
    },
    "financial-services": {
        "net interest margin", "deposit beta", "fee income", "trading revenue",
        "underwriting", "charge-offs", "delinquency rate", "provision for credit losses",
        "capital ratio", "fund flows", "aum", "interchange"
    },
    "healthcare": {
        "fda approval", "phase 1", "phase 2", "phase 3", "label expansion",
        "biosimilar", "510(k)", "pma", "recall", "formulary access",
        "reimbursement", "medical loss ratio", "star ratings"
    },
    "industrials": {
        "backlog", "order intake", "book-to-bill", "utilization",
        "capacity", "labor shortage", "supply chain", "operating ratio",
        "psr", "freight rates", "intermodal", "lead times"
    },
    "technology": {
        "saas", "arr", "net retention", "churn", "bookings",
        "dau", "mau", "ai copilots", "design wins", "node shrink",
        "hbm", "gpu demand", "cloud migration", "zero trust", "observability"
    },
    "utilities": {
        "rate case", "allowed roe", "capex plan", "load growth",
        "storm cost recovery", "outage", "interconnection queue",
        "capacity market", "ppa", "grid hardening"
    },
    "basic-materials": {
        "lme prices", "spot price", "spreads", "smelting",
        "inventories", "grades", "tariffs", "capacity utilization",
        "turnarounds", "input costs"
    },
    "real-estate": {
        "ffo", "noi", "occupancy", "rent spreads", "leasing",
        "same-property", "cap rates", "nav discount", "debt maturities",
        "net absorption", "concessions"
    },
}

# --- Helpers ---
def _norm(s: str | None) -> str:
    return (s or "").strip().lower()

def _slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", _norm(s)).strip("-")

def _clean_html_to_text(html: str | None) -> str:
    if not html:
        return ""
    return BeautifulSoup(html, "html.parser").get_text(separator=" ", strip=True)

UTC = datetime.timezone.utc

def _parse_iso_utc(ts: str | None) -> datetime.datetime | None:
    """Parse ISO8601 timestamps that may end with 'Z' into AWARE UTC datetimes."""
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.datetime.fromisoformat(ts).astimezone(UTC)
    except Exception:
        return None

def _is_recent(ts: str | None, cutoff: datetime.datetime) -> bool:
    """True if ts >= cutoff (both aware UTC)."""
    dt = _parse_iso_utc(ts)
    return bool(dt and dt >= cutoff)

def _mk_set_from_env(csv: str | None) -> Set[str]:
    return {x.strip().lower() for x in (csv or "").split(",") if x.strip()}

ALLOW_SET = _mk_set_from_env(PUBLISHER_ALLOW)
BLOCK_SET = _mk_set_from_env(PUBLISHER_BLOCK)

def _publisher_passes(a: dict) -> bool:
    name = _norm((a.get("publisher") or {}).get("name"))
    if name and any(b in name for b in BLOCK_SET):
        return False
    if ALLOW_SET:
        return any(w in name for w in ALLOW_SET)
    return True

def _title_is_noise(a: dict) -> bool:
    title = _norm(a.get("title"))
    return any(t in title for t in TITLE_EXCLUDES)

# --- BigQuery helpers ---
def get_sector_industry_map(bq_client: bigquery.Client, tickers: List[str]) -> Dict[str, Dict[str, str]]:
    if not tickers:
        return {}
    query = f"""
        SELECT ticker, industry, sector
        FROM (
            SELECT ticker, industry, sector,
                   ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY quarter_end_date DESC) rn
            FROM `{STOCK_METADATA_TABLE_ID}`
            WHERE ticker IN UNNEST(@tickers)
        ) WHERE rn = 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tickers", "STRING", tickers)]
    )
    df = bq_client.query(query, job_config=job_config).to_dataframe()
    return {r["ticker"]: {"industry": r["industry"], "sector": r["sector"]} for _, r in df.iterrows()}

# --- Fetch macro pool for the last 24 hours (Polygon v2/reference/news) ---
def fetch_v2_macro_pool(client: PolygonClient) -> List[dict]:
    now_utc = datetime.datetime.now(tz=UTC).replace(microsecond=0)
    cutoff = (now_utc - datetime.timedelta(hours=24))
    url = f"{client.BASE}/v2/reference/news"
    params = {
        "order": "desc",
        "limit": V2_PER_PAGE,
        "published_utc.gte": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "published_utc.lte": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    try:
        j = client._get(url, params=params)
        arts = j.get("results", []) or []
        filtered = [
            a for a in arts
            if _is_recent(a.get("published_utc"), cutoff)
            and _publisher_passes(a)
            and not _title_is_noise(a)
        ]
        logging.info(f"Fetched {len(filtered)} filtered macro articles (last 24h).")
        return filtered
    except Exception as e:
        logging.error(f"Failed to fetch macro pool: {e}")
        return []

# --- Scoring ---
def _score(a: dict, keys: Iterable[str]) -> int:
    txt = _norm((a.get("title") or "") + " " + (a.get("description") or ""))
    return sum(1 for k in keys if k in txt)

def score_and_select_macro(arts: List[dict], sector_slug: str) -> Tuple[List[dict], List[dict]]:
    """Scores by sector (sector-level keywords) + global macro keywords."""
    sector_keys = SECTOR_KEYWORDS.get(sector_slug, set())

    global_hits = [a for a in arts if _score(a, MACRO_KEYWORDS) >= MIN_GLOBAL_SCORE]
    sector_hits = [a for a in arts if sector_keys and _score(a, sector_keys) >= MIN_SECTOR_SCORE]

    global_sorted = sorted(global_hits, key=lambda x: _score(x, MACRO_KEYWORDS), reverse=True)
    sector_sorted = sorted(sector_hits, key=lambda x: _score(x, sector_keys), reverse=True)
    return global_sorted[:MACRO_GLOBAL_LIMIT], sector_sorted[:MACRO_SECTOR_LIMIT]

# --- Per-ticker worker ---
def fetch_and_save_headlines(
    ticker: str,
    metadata_map: Dict[str, Dict[str, str]],
    macro_pool: List[dict],
    polygon_client: PolygonClient,
    storage_client: storage.Client,
):
    now_utc = datetime.datetime.now(tz=UTC).replace(microsecond=0)
    cutoff = now_utc - datetime.timedelta(hours=24)
    try:
        # --- 1) Ticker-specific (Polygon v2/reference/news) ---
        stock_raw = polygon_client.fetch_news(ticker=ticker, limit_per_page=TICKER_NEWS_LIMIT * 5)
        stock_filtered = [
            a for a in (stock_raw or [])
            if _is_recent(a.get("published_utc"), cutoff)
            and _publisher_passes(a)
            and not _title_is_noise(a)
        ]
        stock_news = [{
            "title": a.get("title"),
            "publishedDate": a.get("published_utc"),
            "text": _clean_html_to_text(a.get("description")),
            "url": a.get("article_url"),
        } for a in stock_filtered[:TICKER_NEWS_LIMIT]]

        # --- 2) Macro (global + sector) ---
        sector = metadata_map.get(ticker, {}).get("sector", "")
        sector_slug = _slugify(sector)
        global_macro, sector_macro = score_and_select_macro(macro_pool, sector_slug)

        # Dedupe + ensure last 24h
        seen: Dict[str, dict] = {}
        for a in (global_macro + sector_macro):
            if not _is_recent(a.get("published_utc"), cutoff):
                continue
            u = a.get("article_url")
            if u and u not in seen:
                seen[u] = a

        macro_news = [{
            "title": a.get("title"),
            "publishedDate": a.get("published_utc"),
            "text": _clean_html_to_text(a.get("description")),
            "url": a.get("article_url"),
        } for a in seen.values()]

        # --- 3) Save (match analyzer: TICKER_YYYY-MM-DD.json) ---
        out_date = now_utc.date().isoformat()
        output_path = f"{NEWS_OUTPUT_PREFIX}{ticker}_{out_date}.json"
        gcs.cleanup_old_files(storage_client, NEWS_OUTPUT_PREFIX, ticker, output_path)
        gcs.upload_json_to_gcs(storage_client, {"stock_news": stock_news, "macro_news": macro_news}, output_path)

        logging.info(f"[{ticker}] saved {len(stock_news)} stock + {len(macro_news)} macro (last 24h).")
        return ticker
    except Exception as e:
        logging.error(f"[{ticker}] failed: {e}", exc_info=True)
        return None

# --- Entry ---
def run_pipeline():
    if not POLYGON_API_KEY:
        logging.critical("POLYGON_API_KEY not set. aborting.")
        return
    logging.info("--- Starting daily news_fetcher (last 24h, sector keywords) ---")
    storage_client = storage.Client()
    bq_client = bigquery.Client()
    polygon_client = PolygonClient(api_key=POLYGON_API_KEY)

    # THIS IS THE CHANGE: Use the full ticker list from GCS
    tickers = gcs.get_tickers(storage_client)
    if not tickers:
        logging.warning("No tickers found in tickerlist.txt. exiting.")
        return

    metadata_map = get_sector_industry_map(bq_client, tickers)
    macro_pool = fetch_v2_macro_pool(polygon_client)

    processed = 0
    # Adjusted max_workers to use a safe default if not in config
    max_workers = config.MAX_WORKERS_TIERING.get("news_fetcher", 8)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(fetch_and_save_headlines, t, metadata_map, macro_pool, polygon_client, storage_client): t
            for t in tickers
        }
        for f in as_completed(futures):
            if f.result():
                processed += 1
    logging.info(f"--- News Fetcher Finished. Processed {processed}/{len(tickers)} tickers ---")