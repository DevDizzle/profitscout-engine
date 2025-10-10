# ingestion/core/clients/polygon.py
import logging
import time
import requests
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
    """
    Minimal Polygon REST client for snapshots.

    Endpoints used:
      - Option Chain Snapshot: /v3/snapshot/options/{underlyingAsset}
      - Unified Snapshot: /v3/snapshot  (search by 'ticker' only; do NOT combine with 'type')
      - Stocks Single-Ticker Snapshot (v2): /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}
    """
    BASE = "https://api.polygon.io"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("POLYGON_API_KEY is required")
        self.api_key = api_key
        # Conservative default for Starter plans.
        self._rl = _RateLimiter(max_calls=4, period=1.0)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), reraise=True)
    def _get(self, url: str, params: dict | None = None) -> dict:
        """GET with simple rate limiting and API key injection (works with next_url)."""
        self._rl.acquire()
        params = dict(params or {})
        # Polygon REST auth via query string
        params["apiKey"] = self.api_key
        r = requests.get(url, params=params, timeout=30)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            logging.error("Polygon GET %s failed: %s | body=%s", url, e, r.text)
            raise
        return r.json()

    # -------------------------- helpers --------------------------

    @staticmethod
    def _extract_underlying_price(und: dict) -> float | None:
        """
        Try multiple known locations for the underlying stock price inside an options snapshot.
        Payloads differ by plan; probe several fallbacks.
        """
        if not und:
            return None

        # 1) Some payloads include a direct price
        v = und.get("price")
        if isinstance(v, (int, float)):
            return float(v)

        # 2) Session fields
        s = und.get("session") or {}
        for k in ("price", "close", "previous_close", "prev_close"):
            v = s.get(k)
            if isinstance(v, (int, float)):
                return float(v)

        # 3) Last trade on the underlying
        lt = und.get("last_trade") or {}
        v = lt.get("price")
        if isinstance(v, (int, float)):
            return float(v)

        # 4) Last quote midpoint on the underlying
        lq = und.get("last_quote") or {}
        v_mid = lq.get("midpoint")
        if isinstance(v_mid, (int, float)):
            return float(v_mid)
        bid, ask = lq.get("bid"), lq.get("ask")
        if isinstance(bid, (int, float)) and isinstance(ask, (int, float)):
            return (bid + ask) / 2.0

        # 5) Day bar close if exposed
        day = und.get("day") or {}
        v = day.get("close")
        if isinstance(v, (int, float)):
            return float(v)

        return None

    @staticmethod
    def _extract_best_price_fields(r: dict) -> tuple[float | None, float | None, float | None]:
        """Return (last_price, bid, ask) from contract-level fields with sensible fallbacks."""
        day = r.get("day") or {}
        trade = r.get("last_trade") or {}
        quote = r.get("last_quote") or {}

        last_price = trade.get("price") or day.get("close")
        bid = quote.get("bid") or day.get("low")
        ask = quote.get("ask") or day.get("high")
        return last_price, bid, ask

    def _map_result(self, r: dict) -> dict:
        details = r.get("details") or {}
        greeks = r.get("greeks") or {}
        und = r.get("underlying_asset") or {}
        day = r.get("day") or {}

        last_price, bid, ask = self._extract_best_price_fields(r)

        return {
            "contract_symbol": details.get("ticker"),
            "option_type": (details.get("contract_type") or "").lower(),   # 'call'|'put'
            "expiration_date": details.get("expiration_date"),             # YYYY-MM-DD
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
            # robust extraction across plan variations
            "underlying_price": self._extract_underlying_price(und),
        }

    # -------------------------- public API --------------------------

    def fetch_underlying_price(self, ticker: str) -> float | None:
        """
        Get the current price of the underlying stock.
        First try v3 Unified Snapshot with a simple 'ticker' search (no 'type'),
        then fall back to v2 Stocks Single-Ticker Snapshot.
        """
        # --- v3 unified snapshot (no 'type' and no 'ticker.any_of') ---
        url = f"{self.BASE}/v3/snapshot"
        params = {"ticker": ticker, "limit": 5}  # lexicographic search starting at 'ticker'
        try:
            j = self._get(url, params)
            for snap in (j.get("results") or []):
                # ensure exact match and stocks type
                if snap.get("ticker") == ticker and snap.get("type") == "stocks":
                    sess = snap.get("session") or {}
                    price = sess.get("price") or sess.get("close")
                    if not isinstance(price, (int, float)):
                        lt = snap.get("last_trade") or {}
                        lq = snap.get("last_quote") or {}
                        price = lt.get("price") or lq.get("midpoint")
                        if not isinstance(price, (int, float)):
                            bid, ask = lq.get("bid"), lq.get("ask")
                            if isinstance(bid, (int, float)) and isinstance(ask, (int, float)):
                                price = (bid + ask) / 2.0
                    if isinstance(price, (int, float)):
                        return float(price)
        except Exception as e:
            logging.warning("Unified snapshot failed for %s: %s", ticker, e)

        # --- v2 fallback: Single Ticker Snapshot (stocks) ---
        # /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}
        url = f"{self.BASE}/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
        try:
            j = self._get(url, params=None)
            t = (j.get("ticker") or {})
            lt = t.get("lastTrade") or {}
            lq = t.get("lastQuote") or {}
            day = t.get("day") or {}
            price = lt.get("p")
            if not isinstance(price, (int, float)):
                bp, ap = lq.get("bp"), lq.get("ap")
                if isinstance(bp, (int, float)) and isinstance(ap, (int, float)):
                    price = (bp + ap) / 2.0
            if not isinstance(price, (int, float)):
                price = day.get("c")
            if isinstance(price, (int, float)):
                return float(price)
            logging.warning("No usable price in v2 single-ticker snapshot for %s", ticker)
        except Exception as e:
            logging.error("Stocks single-ticker snapshot failed for %s: %s", ticker, e)

        return None

    def fetch_options_chain(self, ticker: str, max_days: int = 90) -> list[dict]:
        """
        Snapshot all active option contracts for an underlying (paged).
        Includes greeks, IV, OI, quotes, trades, and (when your plan allows) underlying_asset info.

        If any contract is missing underlying_price, we backfill once from the stock snapshot.
        """
        url = f"{self.BASE}/v3/snapshot/options/{ticker}"
        params = {"limit": 250}  # Polygon default 10, max 250
        out: list[dict] = []

        today = date.today()
        max_exp = today + timedelta(days=max_days)

        while True:
            j = self._get(url, params=params)
            for r in (j.get("results") or []):
                # client-side ≤ max_days expiry guard
                exp = (r.get("details") or {}).get("expiration_date")
                try:
                    if exp and not (today <= date.fromisoformat(exp) <= max_exp):
                        continue
                except Exception:
                    # ignore bad/missing expiration formats
                    continue
                out.append(self._map_result(r))

            next_url = j.get("next_url")
            if not next_url:
                break
            # next_url is a fully-qualified URL; _get will re-append apiKey
            url, params = next_url, {}

        # Backfill a single time if any contract is missing the underlying price
        if out and any(o.get("underlying_price") is None for o in out):
            upx = self.fetch_underlying_price(ticker)
            if isinstance(upx, (int, float)):
                for o in out:
                    if o.get("underlying_price") is None:
                        o["underlying_price"] = float(upx)
            else:
                logging.warning("Unable to backfill underlying price for %s", ticker)

        return out
