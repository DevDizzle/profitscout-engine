# ingestion/core/clients/polygon_client.py

import time
import requests
from requests.adapters import HTTPAdapter
from datetime import date, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential


class _RateLimiter:
    def __init__(self, max_calls: int, period: float):
        self.max_calls, self.period = max_calls, period
        self._ts = []

    def acquire(self):
        now = time.time()
        self._ts = [t for t in self._ts if now - t < self.period]
        if len(self._ts) >= self.max_calls:
            sleep_for = (self._ts[0] + self.period) - now
            if sleep_for > 0:
                time.sleep(sleep_for)
        self._ts.append(time.time())


class PolygonClient:
    BASE = "https://api.polygon.io"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("POLYGON_API_KEY is required")
        self.api_key = api_key
        # tune by env if desired: POLYGON_RL_MAX_CALLS, POLYGON_RL_PERIOD
        self._rl = _RateLimiter(max_calls=20, period=1.0)

        # connection pooling (faster under threads)
        self._session = requests.Session()
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self._session.mount("https://", adapter)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), reraise=True)
    def _get(self, url: str, params: dict | None = None) -> dict:
        self._rl.acquire()
        params = dict(params or {})
        params["apiKey"] = self.api_key  # Polygon REST auth via query string
        r = self._session.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    # -------------------------
    # Options (unchanged)
    # -------------------------

    def _map_result(self, r: dict) -> dict:
        details = r.get("details") or {}
        greeks = r.get("greeks") or {}
        quote = r.get("last_quote") or {}
        trade = r.get("last_trade") or {}
        day = r.get("day") or {}
        und = (r.get("underlying_asset") or {})
        last_price = trade.get("price", day.get("close"))
        bid = quote.get("bid", day.get("low"))
        ask = quote.get("ask", day.get("high"))

        return {
            "contract_symbol": details.get("ticker"),
            "option_type": (details.get("contract_type") or "").lower(),
            "expiration_date": details.get("expiration_date"),  # YYYY-MM-DD
            "strike": details.get("strike_price"),
            "last_price": last_price,
            "bid": bid,
            "ask": ask,
            "volume": r.get("volume", day.get("volume")),
            "open_interest": r.get("open_interest"),
            "implied_volatility": r.get("implied_volatility"),
            "delta": greeks.get("delta"),
            "theta": greeks.get("theta"),
            "vega": greeks.get("vega"),
            "gamma": greeks.get("gamma"),
            "underlying_price": und.get("price"),
        }

    def fetch_options_chain(self, ticker: str, max_days: int = 90) -> list[dict]:
        url = f"{self.BASE}/v3/snapshot/options/{ticker}"
        params = {"limit": 250}
        out: list[dict] = []
        today = date.today()
        max_exp = today + timedelta(days=max_days)

        while True:
            j = self._get(url, params=params)
            for r in (j.get("results") or []):
                exp = (r.get("details") or {}).get("expiration_date")
                try:
                    if exp and not (today <= date.fromisoformat(exp) <= max_exp):
                        continue
                except Exception:
                    continue
                out.append(self._map_result(r))

            next_url = j.get("next_url")
            if not next_url:
                break
            url, params = next_url, {}
        return out

    # -------------------------
    # Benzinga News via Polygon (unchanged)
    # -------------------------

    def _as_iso_day(self, d: str, end: bool = False) -> str:
        if not d:
            return d
        if "T" in d:
            return d
        return f"{d}T23:59:59Z" if end else f"{d}T00:00:00Z"

    def _csv(self, s: str | None) -> str | None:
        if not s:
            return s
        parts = [p.strip() for p in s.split(",")]
        parts = [p for p in parts if p]
        return ",".join(parts) if parts else None

    def _page_through(self, start_url: str, start_params: dict, paginate: bool) -> list[dict]:
        local_url, local_params = start_url, dict(start_params)
        acc: list[dict] = []
        while True:
            j = self._get(local_url, local_params)
            acc.extend(j.get("results") or [])
            if not paginate:
                break
            next_url = j.get("next_url")
            if not next_url:
                break
            local_url, local_params = next_url, {}
        return acc

    def _is_macro_like(self, article: dict) -> bool:
        title = (article.get("title") or "").lower()
        teaser = (article.get("teaser") or "").lower()
        body = (article.get("body") or "").lower()
        text = f"{title} {teaser} {body}"
        keywords = (
            "cpi","pce","ppi","core inflation","inflation","deflation",
            "fomc","federal reserve","powell","rate hike","rate cut",
            "interest rate","dot plot","minutes","qe","qt",
            "jobs report","nonfarm payroll","unemployment","jolts",
            "gdp","recession","soft landing","ism","pmi",
            "treasury yield","bond market","boe","ecb","boj",
            "macro","economy","markets","geopolitics","tariff"
        )
        if any(k in text for k in keywords):
            return True
        tickers = article.get("tickers") or []
        return len(tickers) <= 1

    def fetch_news(
        self,
        ticker: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        limit_per_page: int = 1000,
        paginate: bool = True,
        topics_str: str | None = None,
        channels_str: str | None = None,
    ) -> list[dict]:
        url = f"{self.BASE}/benzinga/v1/news"
        params = {"limit": limit_per_page, "sort": "published.desc"}
        if ticker:
            params["tickers"] = ticker
        if from_date:
            params["published.gte"] = self._as_iso_day(from_date, end=False)
        if to_date:
            params["published.lte"] = self._as_iso_day(to_date, end=True)
        preferred_channels = channels_str or topics_str
        if preferred_channels:
            params["channels"] = self._csv(preferred_channels)
        results = self._page_through(url, params, paginate)
        if not results and (topics_str and not channels_str):
            params.pop("channels", None)
            params["tags"] = self._csv(topics_str)
            results = self._page_through(url, params, paginate)
        if not results and not ticker:
            probe = {"limit": max(50, limit_per_page), "sort": "published.desc"}
            if from_date:
                probe["published.gte"] = self._as_iso_day(from_date, end=False)
            if to_date:
                probe["published.lte"] = self._as_iso_day(to_date, end=True)
            broad = self._page_through(url, probe, paginate=False)
            results = [a for a in broad if self._is_macro_like(a)]
            results = results[:limit_per_page]
        return results

    # -------------------------
    # Polygon v2/reference/news (macro)
    # -------------------------

    def fetch_news_v2_macro(
        self,
        from_date: str,
        to_date: str,
        want: int = 4,
        per_page: int = 200,
        publisher_whitelist: list[str] | None = None,
    ) -> list[dict]:
        """
        Fetch macro-like items from /v2/reference/news (faster & broad coverage).
        - Time window via published_utc.gte/lte
        - order=desc, limit up to 1000 supported
        - next_url pagination
        Docs show: max limit=1000, next_url, publisher fields, image_url, description. :contentReference[oaicite:0]{index=0}
        """
        url = f"{self.BASE}/v2/reference/news"
        params = {
            "order": "desc",
            "limit": max(1, min(per_page, 1000)),
            "published_utc.gte": self._as_iso_day(from_date, end=False),
            "published_utc.lte": self._as_iso_day(to_date, end=True),
        }

        macro_keywords = (
            "cpi","pce","ppi","inflation","deflation","disinflation","core inflation",
            "fomc","federal reserve","powell","rate hike","rate cut","interest rate",
            "dot plot","fed minutes","qe","qt",
            "jobs report","nonfarm payroll","nfp","unemployment","jolts",
            "gdp","recession","soft landing","ism","pmi",
            "treasury yield","bond market","curve","inversion",
            "ecb","boj","boe","pboc","geopolitics","tariff","sanction"
        )

        def _is_macro_v2(a: dict) -> bool:
            title = (a.get("title") or "").lower()
            desc = (a.get("description") or "").lower()
            text = f"{title} {desc}"
            return any(k in text for k in macro_keywords)

        def _passes_publisher(a: dict) -> bool:
            if not publisher_whitelist:
                return True
            name = ((a.get("publisher") or {}).get("name") or "").lower()
            wl = [p.lower() for p in publisher_whitelist]
            return any(w in name for w in wl)

        out, seen = [], set()
        while True:
            j = self._get(url, params=params)
            for a in (j.get("results") or []):
                key = a.get("id") or a.get("article_url")
                if key in seen:
                    continue
                seen.add(key)
                if _passes_publisher(a) and _is_macro_v2(a):
                    out.append(a)
                    if len(out) >= want:
                        break
            if len(out) >= want:
                break
            next_url = j.get("next_url")
            if not next_url:
                break
            url, params = next_url, {}
        return out[:want]
