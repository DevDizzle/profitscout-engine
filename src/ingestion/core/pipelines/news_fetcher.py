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
MACRO_NEWS_LIMIT  = int(os.getenv("NEWS_MACRO_LIMIT",  "3"))  # bumped for better macro coverage

# Macro array we emit (sector + macro combined)
NEWS_MACRO_RETURN = int(os.getenv("NEWS_MACRO_RETURN", "10"))

# Polygon v2
V2_PER_PAGE = int(os.getenv("NEWS_V2_PER_PAGE", "100"))
V2_MAX_PAGES = int(os.getenv("NEWS_V2_MAX_PAGES", "5"))

# Publisher allow/block (used in v2 fallback path)
PUBLISHER_ALLOW = os.getenv(
    "NEWS_PUBLISHER_ALLOW",
    "Reuters,Bloomberg,The Wall Street Journal,Financial Times,CNBC,MarketWatch,Yahoo Finance,Benzinga"
)
PUBLISHER_BLOCK = os.getenv(
    "NEWS_PUBLISHER_BLOCK",
    "GlobeNewswire,PR Newswire,Business Wire,Accesswire,Seeking Alpha PR"
)

# --- Benzinga channels (via Polygon /benzinga/v2/news) ---
BENZ_TICKER_CHANNELS = [c.strip() for c in os.getenv(
    "BENZ_TICKER_CHANNELS",
    "wiim,news,earnings,analyst-ratings,guidance"
).split(",") if c.strip()]

BENZ_MACRO_CHANNELS = [c.strip() for c in os.getenv(
    "BENZ_MACRO_CHANNELS",
    "economics,fed,rates,inflation,macro"
).split(",") if c.strip()]

# Hard catalysts (Ticker bucket)
HARD_CATALYST = {
    "earnings", "guidance", "preliminary results", "8-k", "m&a", "acquisition",
    "merger", "dividend", "buyback", "repurchase", "contract win", "terminated contract",
    "downgrade", "upgrade", "price target", "layoffs", "restructuring",
    "outage", "recall", "fda", "approval", "class action settlement"
}

# Macro (kept for v2 fallback scoring)
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

# Expanded excludes to avoid evergreen content on relax path
TITLE_EXCLUDES = {
    "class action", "investors who lost money", "deadline alert", "lawsuit advertising",
    "whale activity", "options activity", "press release",
    "forever stocks", "dividend payer", "how to invest", "etf to buy"
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
# Docs: /v2/reference/news supports ticker/published_utc/limit/order/sort/next_url.
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

        primary = [
            a for a in out
            if _is_recent(a.get("published_utc"), cutoff)
            and _publisher_passes(a, relax=False)
            and not _title_is_noise(a)
        ]
        if primary:
            logging.info(f"Fetched {len(primary)} pool items across {pages} page(s) (allowlist).")
            return primary

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

# --- Benzinga via Polygon helper -------------------------------------------
# IMPORTANT: Benzinga v2 expects `published` filters as epoch seconds OR 'YYYY-MM-DD' (date string).
# We pass simple dates to avoid 400s. Docs + endpoint overview: Polygon Benzinga News, Polygon Stocks News. :contentReference[oaicite:1]{index=1}
def _fetch_benzinga_news(
    client: PolygonClient,
    tickers: List[str] | None,
    channels: List[str] | None,
    published_gte_iso: str,
    published_lte_iso: str,
    limit: int = 500,
    paginate: bool = True,
) -> List[dict]:
    url = f"{client.BASE}/benzinga/v2/news"
    # FIX: convert ISO datetimes to simple YYYY-MM-DD strings
    gte_date = published_gte_iso[:10]
    lte_date = published_lte_iso[:10]

    params = {
        "limit": max(1, min(limit, 50000)),
        "sort": "published.desc",
        "published.gte": gte_date,   # e.g., '2025-10-23'
        "published.lte": lte_date,   # e.g., '2025-10-24'
    }
    if tickers:
        params["tickers"] = ",".join(tickers)
    if channels:
        params["channels"] = ",".join(channels)

    results: List[dict] = []
    local_url, local_params = url, dict(params)
    while True:
        j = client._get(local_url, local_params)
        results.extend(j.get("results") or [])
        if not paginate:
            break
        next_url = j.get("next_url")
        if not next_url:
            break
        local_url, local_params = next_url, {}
    return results

# --- Bucket selectors -------------------------------------------------------

def select_ticker_news(client: PolygonClient, ticker: str, hours: int) -> List[dict]:
    """Benzinga-first ticker headlines; fall back to Polygon v2 strict, then relaxed."""
    now_utc = datetime.datetime.now(tz=UTC).replace(microsecond=0)
    cutoff = (now_utc - datetime.timedelta(hours=hours))
    gte = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    lte = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1) Benzinga first
    bz_raw = _fetch_benzinga_news(
        client,
        tickers=[ticker],
        channels=BENZ_TICKER_CHANNELS,
        published_gte_iso=gte,
        published_lte_iso=lte,
        limit=1000,
        paginate=True,
    )

    def _norm_bz(items: List[dict]) -> List[dict]:
        out = []
        for it in items:
            out.append({
                "title": it.get("title"),
                "publishedDate": it.get("published"),
                "text": (it.get("teaser") or "")[:1000],
                "url": it.get("url"),
                "_channels": [c.lower() for c in (it.get("channels") or [])],
            })
        # Prefer WIIM/Earnings/Guidance/Analyst by simple channel score; then recency
        def _score(x):
            ch = set(x.get("_channels") or [])
            s = 0
            if "wiim" in ch: s += 5
            if "earnings" in ch: s += 4
            if "guidance" in ch: s += 4
            if "analyst-ratings" in ch: s += 3
            if "news" in ch: s += 2
            return (s, x.get("publishedDate") or "")
        out.sort(key=_score, reverse=True)
        return out

    picks = _norm_bz(bz_raw) if bz_raw else []

    # 2) Fallback: Polygon v2 strict allowlist (no relax yet)
    if not picks:
        stock_raw = client.fetch_news(
            ticker=ticker,
            from_date=gte,
            to_date=lte,
            limit_per_page=200,
            paginate=True,
        ) or []

        def _passes_strict(a: dict) -> bool:
            return (
                _is_recent(a.get("published_utc"), cutoff)
                and _publisher_passes(a, relax=False)
                and not _title_is_noise(a)
                and ticker in (a.get("tickers") or [])
            )

        v2 = [{
            "title": a.get("title"),
            "publishedDate": a.get("published_utc"),
            "text": _clean_html_to_text(a.get("description")),
            "url": a.get("article_url"),
        } for a in stock_raw if _passes_strict(a)]
        v2.sort(key=lambda x: x.get("publishedDate") or "", reverse=True)
        picks = v2

    # 3) Last resort: relaxed v2 but exclude evergreen sources
    if not picks:
        RELAX_BLOCK = {"motley fool", "investing.com"}
        stock_raw = client.fetch_news(
            ticker=ticker,
            from_date=gte,
            to_date=lte,
            limit_per_page=200,
            paginate=True,
        ) or []

        def _relaxed(a: dict) -> bool:
            pub = _norm((a.get("publisher") or {}).get("name"))
            if any(b in pub for b in RELAX_BLOCK):
                return False
            return (
                _is_recent(a.get("published_utc"), cutoff)
                and _publisher_passes(a, relax=True)
                and not _title_is_noise(a)
                and ticker in (a.get("tickers") or [])
            )

        v2_relaxed = [{
            "title": a.get("title"),
            "publishedDate": a.get("published_utc"),
            "text": _clean_html_to_text(a.get("description")),
            "url": a.get("article_url"),
        } for a in stock_raw if _relaxed(a)]
        v2_relaxed.sort(key=lambda x: x.get("publishedDate") or "", reverse=True)
        picks = v2_relaxed

    return picks[:TICKER_NEWS_LIMIT]

def select_sector_news_from_pool(pool: List[dict], ticker: str, sector_slug: str, peers: Set[str], hours: int) -> List[dict]:
    """Require (peer ticker ∩ article.tickers) AND sector keyword. (uses v2 pool)"""
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

def select_macro_headlines_from_pool(pool: List[dict], hours: int) -> List[dict]:
    """Use Polygon v2 pool to surface macro headlines (MACRO_TERMS) for backfill."""
    if not pool:
        return []

    now_utc = datetime.datetime.now(tz=UTC).replace(microsecond=0)
    cutoff = (now_utc - datetime.timedelta(hours=hours))

    picks, seen_urls = [], set()
    max_items = max(MACRO_NEWS_LIMIT, NEWS_MACRO_RETURN)

    for a in pool:
        if len(picks) >= max_items:
            break
        if not _is_recent(a.get("published_utc"), cutoff):
            continue
        if not _publisher_passes(a, relax=True) or _title_is_noise(a):
            continue
        if _score_text(a, MACRO_TERMS) < 1:
            continue

        u = a.get("article_url")
        if not u or u in seen_urls:
            continue
        seen_urls.add(u)

        picks.append({
            "title": a.get("title"),
            "publishedDate": a.get("published_utc"),
            "text": _clean_html_to_text(a.get("description")),
            "url": u,
        })

    return picks

def select_macro_news_from_pool(_pool_unused: List[dict], hours: int) -> List[dict]:
    """
    Benzinga macro channels (via Polygon). Signature unchanged; pool is ignored.
    Return 0..MACRO_NEWS_LIMIT items; if none match, return [] (neutral).
    """
    if MACRO_NEWS_LIMIT <= 0:
        return []
    now_utc = datetime.datetime.now(tz=UTC).replace(microsecond=0)
    cutoff = (now_utc - datetime.timedelta(hours=hours))
    gte = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    lte = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        bz = _fetch_benzinga_news(
            client=polygon_client,  # bound in run_pipeline()
            tickers=None,
            channels=BENZ_MACRO_CHANNELS,
            published_gte_iso=gte,
            published_lte_iso=lte,
            limit=1000,
            paginate=True,
        )
    except NameError:
        logging.error("polygon_client not initialized for macro fetch.")
        return []

    out, seen = [], set()
    for it in bz:
        u = it.get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        out.append({
            "title": it.get("title"),
            "publishedDate": it.get("published"),
            "text": (it.get("teaser") or "")[:1000],
            "url": u,
        })
        if len(out) >= MACRO_NEWS_LIMIT:
            break
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
        macro_pool_news = select_macro_headlines_from_pool(recent_pool, WINDOW_HOURS)

        def _append_candidates(src_items: List[dict], priority: int, bucket: List[dict], seen: Set[str]):
            for item in src_items:
                u = item.get("url")
                if not u or u in seen:
                    continue
                seen.add(u)
                dt = _parse_iso_utc(item.get("publishedDate"))
                bucket.append({
                    **item,
                    "_priority": priority,
                    "_ts": dt.timestamp() if dt else float("-inf"),
                })

        candidates, seen_urls = [], set()
        _append_candidates(macro_news_only, 0, candidates, seen_urls)
        _append_candidates(macro_pool_news, 1, candidates, seen_urls)
        _append_candidates(sector_news, 2, candidates, seen_urls)

        candidates.sort(key=lambda it: (it["_priority"], -it["_ts"]))
        macro_combined = [
            {k: v for k, v in it.items() if not k.startswith("_")}
            for it in candidates
        ][:NEWS_MACRO_RETURN]

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

    logging.info(f"--- Starting news_fetcher (Benzinga via Polygon + v2 fallback, last {WINDOW_HOURS}h) ---")
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    # Make polygon_client available to select_macro_news_from_pool
    global polygon_client
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
