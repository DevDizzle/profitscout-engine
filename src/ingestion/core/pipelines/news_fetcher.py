import logging
import datetime
import os
import re
from typing import List, Dict, Iterable, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage, bigquery
from bs4 import BeautifulSoup
from .. import config, gcs
from ..clients.polygon_client import PolygonClient

# --- Configuration ---
NEWS_OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
STOCK_METADATA_TABLE_ID = config.MASTER_TABLE_ID  # Use master table from config
UTC = datetime.timezone.utc

# --- Tunables ---
WINDOW_HOURS = int(os.getenv("NEWS_WINDOW_HOURS", "24"))

# Quotas (sector goes into macro bucket for compatibility with your analyzer)
TICKER_NEWS_LIMIT = int(os.getenv("NEWS_TICKER_LIMIT", "5"))
SECTOR_NEWS_LIMIT = int(os.getenv("NEWS_SECTOR_LIMIT", "5"))
MACRO_NEWS_LIMIT  = int(os.getenv("NEWS_MACRO_LIMIT",  "5"))

# Macro array we emit (sector + macro combined)
NEWS_MACRO_RETURN = int(os.getenv("NEWS_MACRO_RETURN", "10"))

# Polygon v2
V2_PER_PAGE = int(os.getenv("NEWS_V2_PER_PAGE", "100"))
V2_MAX_PAGES = int(os.getenv("NEWS_V2_MAX_PAGES", "5"))  # NEW: small cap so we don’t overfetch

# Publisher allow/block
PUBLISHER_ALLOW = os.getenv(
    "NEWS_PUBLISHER_ALLOW",
    # Expanded to include Benzinga; still tight by default
    "Reuters,Bloomberg,The Wall Street Journal,Financial Times,CNBC,MarketWatch,Yahoo Finance,Benzinga"
)
PUBLISHER_BLOCK = os.getenv(
    "NEWS_PUBLISHER_BLOCK",
    "GlobeNewswire,PR Newswire,Business Wire,Accesswire,Seeking Alpha PR"
)

# Hard catalysts (Ticker bucket)
HARD_CATALYST = {
    "earnings", "guidance", "preliminary results", "8-k", "m&a", "acquisition",
    "merger", "dividend", "buyback", "repurchase", "contract win", "terminated contract",
    "downgrade", "upgrade", "price target", "layoffs", "restructuring",
    "outage", "recall", "fda", "approval", "class action settlement"
}

# Macro (light)
MACRO_TERMS = {
    "fomc", "federal reserve", "rate hike", "rate cut", "cpi", "inflation",
    "ppi", "jobs report", "nonfarm payroll", "jolts", "pce"
}

# Sector lexicon (concise)
SECTOR_KEYWORDS = {
    "communication-services": {"ad spend", "cpm", "streaming", "subs", "arpu", "churn"},
    "consumer-cyclical": {"same-store", "comps", "traffic", "revpar", "adr", "preorders"},
    "consumer-defensive": {"price/mix", "private label", "trade-down", "promo"},
    "energy": {"rig count", "frac spread", "day rates", "crack spread", "lng", "turnarounds"},
    "financial-services": {"net interest margin", "nim", "charge-offs", "delinquency", "aum"},
    "healthcare": {"fda", "phase 2", "phase 3", "recall", "reimbursement"},
    "industrials": {"backlog", "book-to-bill", "freight", "intermodal", "lead times"},
    "technology": {"saas", "arr", "retention", "hbm", "gpu", "node", "design wins"},
    "utilities": {"rate case", "capex", "load growth", "grid"},
    "basic-materials": {"lme", "smelting", "grades", "inventories"},
    "real-estate": {"ffo", "noi", "occupancy", "rent spreads", "leasing", "cap rates"},
}

TITLE_EXCLUDES = {
    "class action", "investors who lost money", "deadline alert", "lawsuit advertising",
    "whale activity", "options activity", "press release"
}

# --- Helpers ----------------------------------------------------------------

def _norm(s: str | None) -> str:
    return (s or "").strip().lower()

def _slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", _norm(s)).strip("-")

def _clean_html_to_text(html: str | None) -> str:
    if not html:
        return ""
    return BeautifulSoup(html, "html.parser").get_text(separator=" ", strip=True)

def _parse_iso_utc(ts: str | None) -> datetime.datetime | None:
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.datetime.fromisoformat(ts).astimezone(UTC)
    except Exception:
        return None

def _is_recent(ts: str | None, cutoff: datetime.datetime) -> bool:
    dt = _parse_iso_utc(ts)
    return bool(dt and dt >= cutoff)

def _mk_set_from_csv(csv: str | None) -> Set[str]:
    return {x.strip().lower() for x in (csv or "").split(",") if x.strip()}

ALLOW_SET = _mk_set_from_csv(PUBLISHER_ALLOW)
BLOCK_SET = _mk_set_from_csv(PUBLISHER_BLOCK)

def _publisher_allowed(name: str, relax: bool = False) -> bool:
    n = _norm(name)
    if any(b in n for b in BLOCK_SET):
        return False
    if relax or not ALLOW_SET:
        return True
    return any(w in n for w in ALLOW_SET)

def _publisher_passes(a: dict, relax: bool = False) -> bool:
    name = _norm((a.get("publisher") or {}).get("name"))
    if not name:
        return False if not relax else True
    return _publisher_allowed(name, relax=relax)

def _title_is_noise(a: dict) -> bool:
    title = _norm(a.get("title"))
    return any(t in title for t in TITLE_EXCLUDES)

def _score_text(a: dict, keys: Iterable[str]) -> int:
    txt = _norm((a.get("title") or "") + " " + (a.get("description") or ""))
    return sum(1 for k in keys if k in txt)

# --- BigQuery ---------------------------------------------------------------

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

# --- Polygon v2 pool with pagination & allowlist fallback -------------------
# Docs: /v2/reference/news supports `ticker`, `published_utc` filter modifiers, `limit`, `order`, `sort`, and `next_url` pagination. :contentReference[oaicite:1]{index=1}
def fetch_recent_news_pool(client: PolygonClient, hours: int) -> List[dict]:
    now_utc = datetime.datetime.now(tz=UTC).replace(microsecond=0)
    cutoff = (now_utc - datetime.timedelta(hours=hours))
    url = f"{client.BASE}/v2/reference/news"
    params = {
        "order": "desc",
        "sort": "published_utc",
        "limit": max(1, min(V2_PER_PAGE, 1000)),
        "published_utc.gte": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "published_utc.lte": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    out, pages = [], 0
    try:
        local_url, local_params = url, dict(params)
        while True:
            j = client._get(local_url, local_params)
            arts = j.get("results", []) or []
            out.extend(arts)
            pages += 1
            next_url = j.get("next_url")
            if not next_url or pages >= V2_MAX_PAGES:
                break
            local_url, local_params = next_url, {}

        # Primary filter: allowlist + noise + time
        primary = [
            a for a in out
            if _is_recent(a.get("published_utc"), cutoff)
            and _publisher_passes(a, relax=False)
            and not _title_is_noise(a)
        ]
        if primary:
            logging.info(f"Fetched {len(primary)} pool items across {pages} page(s) (allowlist).")
            return primary

        # Fallback: relax allowlist, keep blocklist + noise filters
        relaxed = [
            a for a in out
            if _is_recent(a.get("published_utc"), cutoff)
            and _publisher_passes(a, relax=True)
            and not _title_is_noise(a)
        ]
        logging.info(f"No items with allowlist; using relaxed pool of {len(relaxed)} across {pages} page(s).")
        return relaxed

    except Exception as e:
        logging.error(f"Failed to fetch recent Polygon v2 news pool: {e}")
        return []

# --- Bucket selectors -------------------------------------------------------

def select_ticker_news(client: PolygonClient, ticker: str, hours: int) -> List[dict]:
    """Strict ticker match. If allowlist yields nothing, relax to blocklist-only."""
    now_utc = datetime.datetime.now(tz=UTC).replace(microsecond=0)
    cutoff = (now_utc - datetime.timedelta(hours=hours))

    stock_raw = client.fetch_news(
        ticker=ticker,
        from_date=cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
        to_date=now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        limit_per_page=50,
        paginate=True,  # allow pagination for sparse tickers
    ) or []

    def _filter(relax: bool) -> List[dict]:
        items = []
        for a in stock_raw:
            if not _is_recent(a.get("published_utc"), cutoff):
                continue
            if not _publisher_passes(a, relax=relax) or _title_is_noise(a):
                continue
            if ticker not in (a.get("tickers") or []):
                continue
            # Prefer hard catalysts, but still accept reputable headlines if limited
            has_hard = _score_text(a, HARD_CATALYST) > 0
            if has_hard or relax:
                items.append(a)
        items.sort(key=lambda x: x.get("published_utc") or "", reverse=True)
        return items

    primary = _filter(relax=False)
    chosen = primary if primary else _filter(relax=True)

    out = [{
        "title": it.get("title"),
        "publishedDate": it.get("published_utc"),
        "text": _clean_html_to_text(it.get("description")),
        "url": it.get("article_url"),
    } for it in chosen[:TICKER_NEWS_LIMIT]]

    return out

def select_sector_news_from_pool(pool: List[dict], ticker: str, sector_slug: str, peers: Set[str], hours: int) -> List[dict]:
    """Require (peer ticker ∩ article.tickers) AND sector keyword."""
    now_utc = datetime.datetime.now(tz=UTC).replace(microsecond=0)
    cutoff = (now_utc - datetime.timedelta(hours=hours))
    sector_keys = SECTOR_KEYWORDS.get(sector_slug, set())

    picks, seen_urls = [], set()
    for a in pool:
        if len(picks) >= SECTOR_NEWS_LIMIT:
            break
        if not _is_recent(a.get("published_utc"), cutoff):
            continue
        if not _publisher_passes(a, relax=False) or _title_is_noise(a):
            continue
        atickers = set(a.get("tickers") or [])
        if not atickers:
            continue
        if not (atickers & (peers - {ticker})):
            continue
        if sector_keys and _score_text(a, sector_keys) < 1:
            continue
        u = a.get("article_url")
        if not u or u in seen_urls:
            continue
        seen_urls.add(u)
        picks.append(a)

    # Fallback: if empty, relax publisher allowlist for sector too
    if not picks:
        for a in pool:
            if len(picks) >= SECTOR_NEWS_LIMIT:
                break
            if not _is_recent(a.get("published_utc"), cutoff):
                continue
            if not _publisher_passes(a, relax=True) or _title_is_noise(a):
                continue
            atickers = set(a.get("tickers") or [])
            if not atickers or not (atickers & (peers - {ticker})):
                continue
            if sector_keys and _score_text(a, sector_keys) < 1:
                continue
            u = a.get("article_url")
            if not u or u in seen_urls:
                continue
            seen_urls.add(u)
            picks.append(a)

    out = [{
        "title": it.get("title"),
        "publishedDate": it.get("published_utc"),
        "text": _clean_html_to_text(it.get("description")),
        "url": it.get("article_url"),
    } for it in picks]
    return out

def select_macro_news_from_pool(pool: List[dict], hours: int) -> List[dict]:
    """At most 1 reputable macro item; if none, relax publisher filter."""
    if MACRO_NEWS_LIMIT <= 0:
        return []
    now_utc = datetime.datetime.now(tz=UTC).replace(microsecond=0)
    cutoff = (now_utc - datetime.timedelta(hours=hours))

    def _pick(relax: bool) -> List[dict]:
        picks, seen = [], set()
        for a in pool:
            if len(picks) >= MACRO_NEWS_LIMIT:
                break
            if not _is_recent(a.get("published_utc"), cutoff):
                continue
            if not _publisher_passes(a, relax=relax) or _title_is_noise(a):
                continue
            if _score_text(a, MACRO_TERMS) < 1:
                continue
            u = a.get("article_url")
            if not u or u in seen:
                continue
            seen.add(u)
            picks.append(a)
        return picks

    primary = _pick(relax=False)
    chosen = primary if primary else _pick(relax=True)

    out = [{
        "title": it.get("title"),
        "publishedDate": it.get("published_utc"),
        "text": _clean_html_to_text(it.get("description")),
        "url": it.get("article_url"),
    } for it in chosen]
    return out

# --- Per-ticker worker ------------------------------------------------------

def fetch_and_save_headlines(
    ticker: str,
    metadata_map: Dict[str, Dict[str, str]],
    recent_pool: List[dict],
    polygon_client: PolygonClient,
    storage_client: storage.Client,
):
    now_utc = datetime.datetime.now(tz=UTC).replace(microsecond=0)
    out_date = now_utc.date().isoformat()

    try:
        meta = metadata_map.get(ticker, {}) or {}
        sector = meta.get("sector", "") or ""
        sector_slug = _slugify(sector) if sector else ""

        # Peers: other tickers in same sector (cap 10)
        peers = {t for t, m in metadata_map.items() if (m or {}).get("sector", "") == sector}
        if len(peers) > 10:
            peers = set(list(peers)[:10])

        stock_news = select_ticker_news(polygon_client, ticker, WINDOW_HOURS)
        sector_news = select_sector_news_from_pool(recent_pool, ticker, sector_slug, peers, WINDOW_HOURS)
        macro_news_only = select_macro_news_from_pool(recent_pool, WINDOW_HOURS)

        # Sector first, then macro, cap total
        macro_combined = (sector_news + macro_news_only)[:NEWS_MACRO_RETURN]

        output_path = f"{NEWS_OUTPUT_PREFIX}{ticker}_{out_date}.json"
        gcs.cleanup_old_files(storage_client, NEWS_OUTPUT_PREFIX, ticker, output_path)
        gcs.upload_json_to_gcs(
            storage_client,
            {"stock_news": stock_news, "macro_news": macro_combined},
            output_path,
        )

        logging.info(f"[{ticker}] saved {len(stock_news)} stock + {len(macro_combined)} macro (sector+macro).")
        return ticker

    except Exception as e:
        logging.error(f"[{ticker}] failed: {e}", exc_info=True)
        return None

# --- Entry ------------------------------------------------------------------

def run_pipeline():
    if not POLYGON_API_KEY:
        logging.critical("POLYGON_API_KEY not set. aborting.")
        return

    logging.info(f"--- Starting news_fetcher (Polygon News v2, last {WINDOW_HOURS}h) ---")
    storage_client = storage.Client()
    bq_client = bigquery.Client()
    polygon_client = PolygonClient(api_key=POLYGON_API_KEY)

    tickers = gcs.get_tickers(storage_client)
    if not tickers:
        logging.warning("No tickers found in tickerlist.txt. exiting.")
        return

    metadata_map = get_sector_industry_map(bq_client, tickers)
    recent_pool = fetch_recent_news_pool(polygon_client, WINDOW_HOURS)

    processed = 0
    max_workers = config.MAX_WORKERS_TIERING.get("news_fetcher", 8)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(
                fetch_and_save_headlines,
                t, metadata_map, recent_pool, polygon_client, storage_client
            ): t
            for t in tickers
        }
        for f in as_completed(futures):
            if f.result():
                processed += 1

    logging.info(f"--- News Fetcher Finished. Processed {processed}/{len(tickers)} tickers ---")
