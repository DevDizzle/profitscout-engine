import logging
import time
from datetime import date
import requests
from requests.adapters import HTTPAdapter
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
    """
    Polygon REST client for options snapshots (Enrichment-scoped).
    """

    BASE = "https://api.polygon.io"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("POLYGON_API_KEY is required")
        self.api_key = api_key.strip()
        self._rl = _RateLimiter(max_calls=20, period=1.0)
        self._session = requests.Session()
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self._session.mount("https://", adapter)

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), reraise=True
    )
    def _get(self, url: str, params: dict | None = None) -> dict:
        self._rl.acquire()
        params = dict(params or {})
        params["apiKey"] = self.api_key
        try:
            r = self._session.get(url, params=params, timeout=30)
            r.raise_for_status()
        except requests.HTTPError as e:
            logging.error("Polygon GET %s failed: %s | body=%s", url, e, r.text)
            raise
        return r.json()

    @staticmethod
    def _extract_underlying_price(und: dict) -> float | None:
        if not und:
            return None
        v = und.get("price")
        if isinstance(v, (int, float)):
            return float(v)
        s = und.get("session") or {}
        for k in ("price", "close", "previous_close", "prev_close"):
            v = s.get(k)
            if isinstance(v, (int, float)):
                return float(v)
        lt = und.get("last_trade") or {}
        v = lt.get("price")
        if isinstance(v, (int, float)):
            return float(v)
        lq = und.get("last_quote") or {}
        v_mid = lq.get("midpoint")
        if isinstance(v_mid, (int, float)):
            return float(v_mid)
        bid, ask = lq.get("bid"), lq.get("ask")
        if isinstance(bid, (int, float)) and isinstance(ask, (int, float)):
            return (bid + ask) / 2.0
        day = und.get("day") or {}
        v = day.get("close")
        if isinstance(v, (int, float)):
            return float(v)
        return None

    @staticmethod
    def _extract_best_price_fields(
        r: dict,
    ) -> tuple[float | None, float | None, float | None]:
        day = r.get("day") or {}
        trade = r.get("last_trade") or {}
        quote = r.get("last_quote") or {}

        last_price = trade.get("price") or day.get("close")
        bid = quote.get("bid") or day.get("low")
        ask = quote.get("ask") or day.get("high")
        return last_price, bid, ask

    def _map_options_result(self, r: dict) -> dict:
        details = r.get("details") or {}
        greeks = r.get("greeks") or {}
        und = r.get("underlying_asset") or {}
        day = r.get("day") or {}

        last_price, bid, ask = self._extract_best_price_fields(r)

        return {
            "contract_symbol": details.get("ticker"),
            "option_type": (details.get("contract_type") or "").lower(),
            "expiration_date": details.get("expiration_date"),
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
            "underlying_price": self._extract_underlying_price(und),
        }

    def fetch_stock_snapshot(self, ticker: str) -> dict | None:
        url = f"{self.BASE}/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
        try:
            return self._get(url)
        except Exception as e:
            logging.error("Stocks snapshot failed for %s: %s", ticker, e)
            return None

    def fetch_option_contract_snapshot(
        self, underlying_asset: str, contract_symbol: str
    ) -> dict | None:
        url = f"{self.BASE}/v3/snapshot/options/{underlying_asset}/{contract_symbol}"
        try:
            res = self._get(url)
            r = res.get("results")
            if r:
                return self._map_options_result(r)
            return None
        except Exception as e:
            logging.error(
                "Option contract snapshot failed for %s: %s", contract_symbol, e
            )
            return None

    def fetch_all_tickers_snapshot(self) -> list[dict]:
        """
        Get snapshot for ALL stock tickers in one call.
        /v2/snapshot/locale/us/markets/stocks/tickers
        """
        url = f"{self.BASE}/v2/snapshot/locale/us/markets/stocks/tickers"
        try:
            res = self._get(url)
            return res.get("tickers") or []
        except Exception as e:
            logging.error("All-tickers snapshot failed: %s", e)
            return []

    def fetch_underlying_price(self, ticker: str) -> float | None:
        """
        Fetch current/latest price for a ticker (for backfilling options data).
        """
        try:
            snap = self.fetch_stock_snapshot(ticker)
            if not snap:
                return None
            t = snap.get("ticker") or {}
            # 1. lastTrade.p
            lt = t.get("lastTrade") or {}
            p = lt.get("p")
            if p is not None: return float(p)
            # 2. day.c
            day = t.get("day") or {}
            c = day.get("c")
            if c is not None: return float(c)
            # 3. prevDay.c
            pd = t.get("prevDay") or {}
            pc = pd.get("c")
            if pc is not None: return float(pc)
            
            return self._extract_underlying_price(t)
        except Exception:
            return None

    def fetch_options_chain(self, ticker: str, max_days: int = 90) -> list[dict]:
        """
        Snapshot all active option contracts for an underlying (paged).
        """
        from datetime import timedelta

        url = f"{self.BASE}/v3/snapshot/options/{ticker}"
        params = {"limit": 250}
        out: list[dict] = []
        today = date.today()
        max_exp = today + timedelta(days=max_days)

        while True:
            j = self._get(url, params=params)
            for r in j.get("results") or []:
                exp = (r.get("details") or {}).get("expiration_date")
                try:
                    if exp and not (today <= date.fromisoformat(exp) <= max_exp):
                        continue
                except Exception:
                    continue
                out.append(self._map_options_result(r))

            next_url = j.get("next_url")
            if not next_url:
                break
            url, params = next_url, {}

        # Backfill underlying price if missing
        if out and any(o.get("underlying_price") is None for o in out):
            upx = self.fetch_underlying_price(ticker)
            if isinstance(upx, (int, float)):
                for o in out:
                    if o.get("underlying_price") is None:
                        o["underlying_price"] = float(upx)

        return out
