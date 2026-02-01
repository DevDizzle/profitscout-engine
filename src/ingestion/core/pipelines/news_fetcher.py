import datetime
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup
from google.cloud import storage

from .. import config, gcs
from ..clients.polygon_client import PolygonClient

# --- Configuration ---
NEWS_OUTPUT_PREFIX = config.PREFIXES["news_analyzer"]["input"]
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
UTC = datetime.UTC

# --- Tunables ---
WINDOW_HOURS = int(os.getenv("NEWS_WINDOW_HOURS", "24"))
TICKER_NEWS_LIMIT = int(os.getenv("NEWS_TICKER_LIMIT", "10"))

# --- Helpers ---


def _norm(s: str | None) -> str:
    return (s or "").strip().lower()


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


# --- Fetching Logic ---


def fetch_ticker_news(client: PolygonClient, ticker: str, hours: int) -> list[dict]:
    """
    Fetches strict, recent news for a single ticker.
    Prioritizes Benzinga (richer context) then Polygon v2.
    """
    now_utc = datetime.datetime.now(tz=UTC).replace(microsecond=0)
    cutoff = now_utc - datetime.timedelta(hours=hours)
    gte = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    lte = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    gte_date = gte[:10]
    lte_date = lte[:10]

    # 1. Benzinga (via Polygon) - Better for catalysts
    url_bz = f"{client.BASE}/benzinga/v2/news"
    params_bz = {
        "tickers": ticker,
        "limit": 50,
        "sort": "published.desc",
        "published.gte": gte_date,
        "published.lte": lte_date,
    }

    picks = []

    try:
        bz_res = client._get(url_bz, params_bz)
        items = bz_res.get("results") or []
        for it in items:
            # Re-check recency strictly against timestamp
            if _is_recent(it.get("published"), cutoff):
                picks.append(
                    {
                        "title": it.get("title"),
                        "publishedDate": it.get("published"),
                        "text": (it.get("teaser") or "")[:1000],
                        "url": it.get("url"),
                        "source": "Benzinga",
                    }
                )
    except Exception as e:
        logging.warning(f"[{ticker}] Benzinga fetch failed: {e}")

    # 2. Polygon Reference v2 (Fallback/Supplement)
    # Only if we have very few items
    if len(picks) < 5:
        try:
            poly_res = (
                client.fetch_news(
                    ticker=ticker,
                    from_date=gte,
                    to_date=lte,
                    limit_per_page=50,
                    paginate=False,
                )
                or []
            )

            for it in poly_res:
                if _is_recent(it.get("published_utc"), cutoff):
                    # Avoid duplicates by URL or Title
                    if any(p["url"] == it.get("article_url") for p in picks):
                        continue

                    picks.append(
                        {
                            "title": it.get("title"),
                            "publishedDate": it.get("published_utc"),
                            "text": _clean_html_to_text(it.get("description")),
                            "url": it.get("article_url"),
                            "source": it.get("publisher", {}).get("name", "Polygon"),
                        }
                    )
        except Exception as e:
            logging.warning(f"[{ticker}] Polygon v2 fetch failed: {e}")

    # Sort by date descending
    picks.sort(key=lambda x: x.get("publishedDate") or "", reverse=True)
    return picks[:TICKER_NEWS_LIMIT]


def fetch_and_save(
    ticker: str, polygon_client: PolygonClient, storage_client: storage.Client
):
    try:
        stock_news = fetch_ticker_news(polygon_client, ticker, WINDOW_HOURS)

        # We write even if empty so the Analyzer knows to do a "quiet check" or skip.
        # Minimal schema.
        output_data = {
            "stock_news": stock_news,
            "macro_news": [],  # Empty list to satisfy downstream schema if needed
        }

        now_utc = datetime.datetime.now(tz=UTC).replace(microsecond=0)
        out_date = now_utc.date().isoformat()
        output_path = f"{NEWS_OUTPUT_PREFIX}{ticker}_{out_date}.json"

        # Cleanup old files first
        gcs.cleanup_old_files(storage_client, NEWS_OUTPUT_PREFIX, ticker, output_path)

        gcs.upload_json_to_gcs(
            storage_client,
            output_data,
            output_path,
        )
        return ticker
    except Exception as e:
        logging.error(f"[{ticker}] failed: {e}")
        return None


# --- Entry ---


def run_pipeline():
    if not POLYGON_API_KEY:
        logging.critical("POLYGON_API_KEY not set. aborting.")
        return

    logging.info(f"--- Starting Simple News Fetcher (Last {WINDOW_HOURS}h) ---")
    storage_client = storage.Client()
    polygon_client = PolygonClient(api_key=POLYGON_API_KEY)

    tickers = gcs.get_tickers(storage_client)
    if not tickers:
        logging.warning("No tickers found in tickerlist.txt. exiting.")
        return

    processed = 0
    # Higher concurrency since requests are simpler
    max_workers = 16

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(fetch_and_save, t, polygon_client, storage_client): t
            for t in tickers
        }
        for f in as_completed(futures):
            if f.result():
                processed += 1

    logging.info(
        f"--- News Fetcher Finished. Processed {processed}/{len(tickers)} tickers ---"
    )
