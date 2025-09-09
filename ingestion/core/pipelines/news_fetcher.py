# ingestion/core/pipelines/news_fetcher.py
import logging
from concurrent.futures import ThreadPoolExecutor
import datetime
import json
import os
import re
from typing import List, Dict, Iterable, Set, Tuple

from google.cloud import storage
from google.cloud import bigquery
from bs4 import BeautifulSoup  # ensure in requirements
from .. import config, gcs
from ..clients.polygon_client import PolygonClient

NEWS_OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

# =========================
# Tunable limits (env)
# =========================
TICKER_NEWS_LIMIT = int(os.getenv("NEWS_TICKER_LIMIT", "2"))         # Benzinga per ticker
MACRO_GLOBAL_LIMIT = int(os.getenv("NEWS_MACRO_GLOBAL_LIMIT", "2"))  # v2 global macro per ticker
MACRO_SECTOR_LIMIT = int(os.getenv("NEWS_MACRO_SECTOR_LIMIT", "2"))  # v2 sector-aware macro per ticker
V2_PER_PAGE = int(os.getenv("NEWS_V2_PER_PAGE", "400"))              # v2/reference/news supports <=1000
V2_LOOKBACK_DAYS = int(os.getenv("NEWS_V2_LOOKBACK_DAYS", "7"))

# Publisher allow / block (case-insensitive substring match)
PUBLISHER_ALLOW = os.getenv(
    "NEWS_PUBLISHER_ALLOW",
    "Reuters,CNBC,MarketWatch,The Wall Street Journal,Financial Times,Bloomberg,Investing.com,Barron's,Benzinga,Yahoo Finance"
)
PUBLISHER_BLOCK = os.getenv(
    "NEWS_PUBLISHER_BLOCK",
    "GlobeNewswire,PR Newswire,Business Wire,Accesswire,Seeking Alpha PR"
)

TITLE_EXCLUDES = {
    "class action","shareholder rights","investors who lost money","deadline alert",
    "lawsuit","whale activity","unusual options","options activity"
}

# =========================
# Global macro lexicon
# =========================
MACRO_KEYWORDS = {
    "cpi","pce","ppi","inflation","deflation","disinflation","core inflation",
    "fomc","federal reserve","powell","rate hike","rate cut","interest rate",
    "dot plot","fed minutes","qe","qt",
    "jobs report","nonfarm payroll","nfp","unemployment","jolts",
    "gdp","recession","soft landing","ism","pmi",
    "treasury yield","bond market","curve","inversion",
    "ecb","boj","boe","pboc","geopolitics","tariff","sanction"
}

# =========================
# Sector lexicon — from you
# (Paste/extend as you build it out)
# Keys should match slugified industry names, e.g. "software-application"
# =========================
SECTOR_KEYWORDS = {
    "software-application": {"saas", "arr", "recurring revenue", "churn rate", "user growth", "app store", "google play", "digital transformation", "ai integration", "enterprise software", "subscription model", "cloud adoption", "cyber threat", "data privacy", "gdpr", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "software-infrastructure": {"cloud computing", "aws", "azure", "data center", "cybersecurity", "devops", "api", "kubernetes", "serverless", "edge computing", "hybrid cloud", "ransomware", "zero trust", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "semiconductors": {"chip shortage", "fab capacity", "tsmc", "asml", "lithography", "euv", "node shrink", "supply chain", "tariff", "china export", "ai chip", "gpu", "nvidia", "intel", "amd", "wafer", "foundry", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "industrial-machinery": {"capex", "factory orders", "automation", "robotics", "supply chain", "tariffs", "steel prices", "manufacturing pmi", "industrial production", "machinery backlog", "oem", "aftermarket", "caterpillar", "deere", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "banks-regional": {"net interest margin", "deposit beta", "loan growth", "credit quality", "fed funds", "basel iii", "fdic", "non-performing loans", "provision", "capital ratio", "community reinvestment", "merger approval", "fomc", "rate cut", "cpi", "gdp", "pmi", "unemployment", "treasury yield"},
    "regulated-electric": {"rate case", "ferc", "power grid", "renewable mandate", "coal plant", "natural gas", "blackout", "demand response", "transmission line", "utility capex", "puc", "epa", "hurricane", "storm", "weather", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "specialty-retail": {"same store sales", "foot traffic", "consumer spending", "inventory turnover", "e-commerce", "tariffs", "holiday sales", "black friday", "supply chain", "retail apocalypse", "omnichannel", "private label", "fomc", "rate cut", "cpi", "gdp", "pmi", "unemployment", "treasury yield"},
    "aerospace-defense": {"defense budget", "faa", "nasa", "aircraft orders", "backlog", "supply chain", "geopolitics", "missile", "fighter jet", "boeing", "lockheed", "raytheon", "delivery delay", "f-35", "space launch", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "information-technology-services": {"outsourcing", "it consulting", "digital transformation", "cloud services", "cybersecurity", "labor costs", "offshoring", "managed services", "rpo", "accenture", "infosys", "project backlog", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "chemicals-specialty": {"feedstock", "oil prices", "natural gas", "tariffs", "supply chain", "environmental regs", "epa", "plastic demand", "agrochemical", "dow chemical", "basf", "commodity cycle", "ethylene", "propylene", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "entertainment": {"box office", "streaming subs", "content costs", "disney", "netflix", "writers strike", "piracy", "cord cutting", "theme park", "sports rights", "hbo", "paramount", "viewer ratings", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "oil-gas-exploration-production": {"wti", "brent", "rig count", "shale", "opec", "fracking", "natural gas", "lng export", "permits", "epa", "drilling", "completion", "eia inventory", "api report", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "biotechnology": {"fda approval", "clinical trials", "phase 3", "pipeline", "biotech index", "funding round", "m&a", "patent cliff", "crispr", "gene therapy", "nih grant", "orphan drug", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "asset-management": {"aum", "management fees", "etf inflows", "outflows", "sec", "fiduciary rule", "market volatility", "blackrock", "vanguard", "passive investing", "hedge fund", "private equity", "fomc", "rate cut", "cpi", "gdp", "pmi", "unemployment", "treasury yield"},
    "medical-diagnostics-research": {"diagnostic test", "covid testing", "fda clearance", "labcorp", "quest diagnostics", "research funding", "nih", "biomarker", "precision medicine", "clinical lab", "reimbursement", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "packaged-foods": {"commodity prices", "wheat", "corn", "food inflation", "supply chain", "fda recall", "organic label", "plant-based", "kellogg", "general mills", "consumer staples", "snack demand", "fomc", "rate cut", "cpi", "gdp", "pmi", "unemployment", "treasury yield"},
    "insurance-property-casualty": {"premium growth", "claims ratio", "catastrophe loss", "reinsurance", "hurricane", "wildfire", "auto insurance", "underwriting", "berkshire", "allstate", "combined ratio", "nat cat", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "construction": {"housing starts", "building permits", "lumber prices", "labor shortage", "infrastructure bill", "interest rates", "non-resi", "commercial real estate", "supply chain", "tariffs", "fomc", "rate cut", "cpi", "gdp", "pmi", "unemployment", "treasury yield"},
    "financial-capital-markets": {"trading volume", "ipo pipeline", "spac", "sec filing", "volatility index", "vix", "m&a activity", "goldman sachs", "morgan stanley", "market maker", "high frequency", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "medical-devices": {"fda approval", "medtech", "implant", "pacemaker", "supply chain", "recall", "reimbursement", "medicare", "johnson johnson", "medtronic", "surgical robot", "da vinci", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "medical-instruments-supplies": {"fda clearance", "syringe", "catheter", "supply chain", "sterilization", "reimbursement", "bd", "baxter", "surgical instrument", "endoscope", "ppe", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "hardware-equipment-parts": {"component shortage", "pcb", "supply chain", "tariffs", "data center", "5g rollout", "cisco", "hp", "dell", "server rack", "network gear", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "financial-credit-services": {"credit card", "delinquency rate", "charge off", "consumer debt", "fico score", "visa", "mastercard", "payment processor", "buy now pay later", "lending", "fomc", "rate cut", "cpi", "gdp", "pmi", "unemployment", "treasury yield"},
    "internet-content-information": {"user engagement", "ad revenue", "algorithm", "content moderation", "google", "meta", "tiktok", "seo", "data privacy", "antitrust", "fomc", "rate cut", "inflation", "gdp", "pmi", "unemployment", "treasury yield"},
    "household-personal-products": {"consumer staples", "supply chain", "commodity prices", "procter gamble", "unilever", "soap", "detergent", "beauty demand", "organic product", "inflation pass through", "fomc", "rate cut", "cpi", "gdp", "pmi", "unemployment", "treasury yield"}
}

# =========================
# Helpers
# =========================
def _norm(s: str | None) -> str:
    return (s or "").strip().lower()

def _mk_set_from_env(csv: str | None) -> Set[str]:
    if not csv:
        return set()
    return {x.strip().lower() for x in csv.split(",") if x.strip()}

ALLOW_SET = _mk_set_from_env(PUBLISHER_ALLOW)
BLOCK_SET = _mk_set_from_env(PUBLISHER_BLOCK)

def _publisher_passes_v2(a: dict) -> bool:
    name = _norm((a.get("publisher") or {}).get("name"))
    if name and any(b in name for b in BLOCK_SET):
        return False
    if ALLOW_SET:
        return any(w in name for w in ALLOW_SET)
    return True

def _title_is_noise(a: dict) -> bool:
    title = _norm(a.get("title"))
    return any(p in title for p in TITLE_EXCLUDES)

def _contains_any(text: str, needles: Iterable[str]) -> bool:
    t = text.lower()
    return any(n in t for n in needles)

def _slugify_industry(s: str) -> str:
    x = _norm(s)
    x = re.sub(r"[^a-z0-9]+", "-", x)
    x = re.sub(r"-{2,}", "-", x).strip("-")
    return x

def _clean_html_to_text(html: str | None) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(separator=" ", strip=True)

# =========================
# BQ: ticker -> (sector, industry)
# =========================
BQ_TABLE = os.getenv("STOCK_METADATA_FQN", "profitscout-lx6bb.profit_scout.stock_metadata")

def get_sector_industry_bq(bq: bigquery.Client, ticker: str) -> Tuple[str, str]:
    query = f"""
    SELECT sector, industry
    FROM `{BQ_TABLE}`
    WHERE ticker = @ticker
    LIMIT 1
    """
    job = bq.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
        ),
    )
    rows = list(job.result())
    if rows:
        return (rows[0].sector or "").strip(), (rows[0].industry or "").strip()
    return "", ""

# =========================
# Mappers
# =========================
def map_benzinga_article(article: dict) -> dict:
    html_text = article.get("body") or article.get("teaser") or ""
    text = _clean_html_to_text(html_text)
    return {
        "title": article.get("title"),
        "publishedDate": article.get("published"),  # ISO
        "image": None,
        "site": article.get("author"),
        "text": text,
        "url": article.get("url"),
    }

def map_polygon_v2_article(article: dict) -> dict:
    desc = article.get("description") or ""
    text = _clean_html_to_text(desc) if ("<" in desc and ">" in desc) else (desc or "")
    publisher = (article.get("publisher") or {}).get("name")
    return {
        "title": article.get("title"),
        "publishedDate": article.get("published_utc"),
        "image": article.get("image_url"),
        "site": publisher,
        "text": text,
        "url": article.get("article_url"),
    }

# =========================
# Fetch macro pool once (v2/reference/news)
# =========================
def _iso_day(d: datetime.date, end: bool) -> str:
    return f"{d.isoformat()}T23:59:59Z" if end else f"{d.isoformat()}T00:00:00Z"

def fetch_v2_macro_pool(client: PolygonClient, days_back: int, per_page: int) -> List[dict]:
    today = datetime.date.today()
    frm = today - datetime.timedelta(days=days_back)

    url = f"{client.BASE}/v2/reference/news"
    params = {
        "order": "desc",
        "limit": max(1, min(per_page, 1000)),  # v2 supports up to 1000
        "published_utc.gte": _iso_day(frm, end=False),
        "published_utc.lte": _iso_day(today, end=True),
    }
    try:
        j = client._get(url, params=params)
        return j.get("results") or []
    except Exception as e:
        logging.error(f"[macro/v2] fetch failed: {e}")
        return []

# =========================
# Ranking
# =========================
def _score_global(a: dict) -> float:
    title = _norm(a.get("title"))
    desc  = _norm(a.get("description"))
    text  = f"{title} {desc}"
    macro_hits = sum(1 for k in MACRO_KEYWORDS if k in text)
    # recency bonus within window
    try:
        ts = a.get("published_utc", "")
        age_days = max(0.0, (datetime.date.today() - datetime.date.fromisoformat(ts[:10])).days)
        recency = max(0.0, 1.0 - (age_days / max(1.0, V2_LOOKBACK_DAYS)))
    except Exception:
        recency = 0.5
    return (2.0 * macro_hits) + (0.75 * recency)

def _score_sector(a: dict, industry_slug: str) -> float:
    title = _norm(a.get("title"))
    desc  = _norm(a.get("description"))
    text  = f"{title} {desc}"

    sector_keys = SECTOR_KEYWORDS.get(industry_slug, set())
    macro_hits  = sum(1 for k in MACRO_KEYWORDS if k in text)
    sector_hits = sum(1 for k in sector_keys if k in text)

    kws = {_norm(k) for k in (a.get("keywords") or [])}
    kw_overlap = len(kws & {k.lower() for k in MACRO_KEYWORDS}) + len(kws & {k.lower() for k in sector_keys})

    try:
        ts = a.get("published_utc", "")
        age_days = max(0.0, (datetime.date.today() - datetime.date.fromisoformat(ts[:10])).days)
        recency = max(0.0, 1.0 - (age_days / max(1.0, V2_LOOKBACK_DAYS)))
    except Exception:
        recency = 0.5

    return (2.0 * macro_hits) + (1.7 * sector_hits) + (1.0 * kw_overlap) + (0.75 * recency)

def _dedup_keep_order(items: List[dict]) -> List[dict]:
    seen = set()
    out = []
    for a in items:
        u = a.get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(a)
    return out

# =========================
# Per-ticker worker
# =========================
def fetch_and_save_headlines(ticker: str, storage_client: storage.Client, bq: bigquery.Client, macro_pool: List[dict]):
    client = PolygonClient(POLYGON_API_KEY)

    today = datetime.date.today()
    from_date = (today - datetime.timedelta(days=V2_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    to_date   = today.strftime("%Y-%m-%d")

    # --- 1) Ticker-specific: Benzinga over Polygon (full text when available)
    # Docs: /benzinga/v1/news supports ticker filter + published.gte/lte
    try:
        stock_news_raw = client.fetch_news(
            ticker=ticker,
            from_date=from_date,
            to_date=to_date,
            limit_per_page=TICKER_NEWS_LIMIT
        )
        stock_news = [map_benzinga_article(a) for a in stock_news_raw][:TICKER_NEWS_LIMIT]
        logging.info(f"[{ticker}] stock (Benzinga) = {len(stock_news)}")
    except Exception as e:
        logging.error(f"[{ticker}] Benzinga fetch failed: {e}")
        stock_news = []

    # --- 2) Sector/Industry lookup
    try:
        sector, industry = get_sector_industry_bq(bq, ticker)
        industry_slug = _slugify_industry(industry) if industry else ""
        logging.info(f"[{ticker}] sector='{sector}' industry='{industry}' slug='{industry_slug}'")
    except Exception as e:
        logging.error(f"[{ticker}] BQ sector/industry lookup failed: {e}")
        sector, industry, industry_slug = "", "", ""

    # --- 3) Macro selection from v2 pool (global + sector-aware)
    filtered_pool = [a for a in macro_pool if _publisher_passes_v2(a) and not _title_is_noise(a)]

    # Global macro
    global_ranked = sorted(filtered_pool, key=_score_global, reverse=True)
    macro_global = [map_polygon_v2_article(a) for a in global_ranked[:MACRO_GLOBAL_LIMIT]]

    # Sector-aware macro
    if industry_slug and industry_slug in SECTOR_KEYWORDS:
        sector_ranked = sorted(filtered_pool, key=lambda a: _score_sector(a, industry_slug), reverse=True)
        macro_sector = [map_polygon_v2_article(a) for a in sector_ranked[:MACRO_SECTOR_LIMIT]]
    else:
        macro_sector = []

    # Combine & dedupe for backward-compat "macro_news"
    macro_news_combined = _dedup_keep_order(macro_global + macro_sector)

    headlines = {
        "stock_news": sorted(stock_news, key=lambda x: x.get("publishedDate", ""), reverse=True),
        "macro_news": sorted(macro_news_combined, key=lambda x: x.get("publishedDate", ""), reverse=True),
        # Optional debug breakdown (comment out if your downstream schema is strict):
        "macro_news_global": macro_global,
        "macro_news_sector": macro_sector,
    }

    output_path = f"{NEWS_OUTPUT_PREFIX}{ticker}_{today.strftime('%Y-%m-%d')}.json"
    gcs.upload_json_to_gcs(storage_client, headlines, output_path)
    gcs.cleanup_old_files(storage_client, NEWS_OUTPUT_PREFIX, ticker, output_path)

    if not headlines["stock_news"] and not headlines["macro_news"]:
        logging.info(f"[{ticker}] No recent news found. Saved empty file.")
        return None

    return output_path

# =========================
# Entry
# =========================
def run_pipeline():
    if not POLYGON_API_KEY:
        logging.critical("POLYGON_API_KEY not set. Aborting news_fetcher.")
        return

    logging.info("--- Starting News Fetcher (Benzinga ticker + sector-aware Polygon v2 macro) ---")
    storage_client = storage.Client()
    bq = bigquery.Client()

    tickers = gcs.get_tickers(storage_client)
    if not tickers:
        logging.warning("No tickers found in tickerlist.txt.")
        return

    # Prefetch macro once (v2/reference/news)
    client = PolygonClient(POLYGON_API_KEY)
    macro_pool = fetch_v2_macro_pool(client, days_back=V2_LOOKBACK_DAYS, per_page=V2_PER_PAGE)
    logging.info(f"[macro pool] v2/reference/news candidates = {len(macro_pool)}")

    max_workers = config.MAX_WORKERS_TIERING.get("news_fetcher", 8)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_and_save_headlines, t, storage_client, bq, macro_pool): t
            for t in tickers
        }
        processed_count = len(futures)

    logging.info(f"--- News Fetcher Finished. Processed {processed_count} tickers. ---")
