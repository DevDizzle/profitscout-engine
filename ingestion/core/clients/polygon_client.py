import logging, time, requests
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
        self._rl = _RateLimiter(max_calls=4, period=1.0)  # starter plan safe defaults

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), reraise=True)
    def _get(self, url: str, params: dict | None = None) -> dict:
        self._rl.acquire()
        params = dict(params or {})
        params["apiKey"] = self.api_key  # Polygon REST auth via query string
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def _map_result(self, r: dict) -> dict:
        details = r.get("details") or {}
        greeks  = r.get("greeks")  or {}
        quote   = r.get("last_quote") or {}
        trade   = r.get("last_trade") or {}
        day     = r.get("day") or {}
        und     = (r.get("underlying_asset") or {})
        # best-effort price fields
        last_price = trade.get("price", day.get("close"))
        bid = quote.get("bid", day.get("low"))
        ask = quote.get("ask", day.get("high"))

        return {
            "contract_symbol": details.get("ticker"),
            "option_type": (details.get("contract_type") or "").lower(),          # 'call'|'put'
            "expiration_date": details.get("expiration_date"),                    # YYYY-MM-DD
            "strike": details.get("strike_price"),
            "last_price": last_price,
            "bid": bid,
            "ask": ask,
            "volume": r.get("volume", day.get("volume")),
            "open_interest": r.get("open_interest"),
            "implied_volatility": r.get("implied_volatility"),
            "delta": greeks.get("delta"),
            "theta": greeks.get("theta"),
            "vega":  greeks.get("vega"),
            "gamma": greeks.get("gamma"),
            "underlying_price": und.get("price"),
        }

    def fetch_options_chain(self, ticker: str, max_days: int = 90) -> list[dict]:
        """
        Snapshot all active option contracts for an underlying (paged).
        Docs: /v3/snapshot/options/{underlyingAsset}; returns greeks, IV, OI, quotes, trades.
        """
        url = f"{self.BASE}/v3/snapshot/options/{ticker}"
        params = {"limit": 250}  # Polygon default 10, max 250
        out: list[dict] = []
        today = date.today()
        max_exp = today + timedelta(days=max_days)

        while True:
            j = self._get(url, params=params)
            for r in (j.get("results") or []):
                # client-side ≤90d expiry guard
                exp = (r.get("details") or {}).get("expiration_date")
                try:
                    if exp and not (today <= date.fromisoformat(exp) <= max_exp):
                        continue
                except Exception:
                    continue
                out.append(self._map_result(r))

            next_url = j.get("next_url")  # requires re-appending apiKey per Polygon docs
            if not next_url:
                break
            url, params = next_url, {}  # apiKey added in _get()
        return out
