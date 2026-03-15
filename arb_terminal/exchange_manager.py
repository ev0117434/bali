"""
ExchangeManager — ccxt wrapper for spot+futures on Binance / Bybit / OKX / Gate.io.

Design
------
* Two ccxt instances per exchange: spot (defaultType='spot') and
  futures (defaultType='swap').
* Instances are created lazily on first use.
* All public methods are **synchronous** (blocking) — ccxt sync API.
* Amount and price are always rounded via ccxt's amount_to_precision /
  price_to_precision before submitting an order.
* Exchange-specific quirks (OKX tdMode, Gate.io contractSize, Bybit
  category) are handled internally.
"""

import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import ccxt

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ccxt class name mapping
# ---------------------------------------------------------------------------

_CCXT_NAME: dict[str, str] = {
    "binance": "binance",
    "bybit": "bybit",
    "gateio": "gateio",
    "okx": "okx",
}

SUPPORTED_EXCHANGES = list(_CCXT_NAME.keys())


# ---------------------------------------------------------------------------
# OrderInfo — snapshot of a single open/closed order
# ---------------------------------------------------------------------------


@dataclass
class OrderInfo:
    order_id: str
    exchange: str
    side: str           # 'buy' | 'sell'
    symbol: str         # ccxt symbol used for this leg
    planned_price: float
    planned_amount: float
    filled: float = 0.0
    avg_price: float = 0.0
    status: str = "open"    # open | closed | canceled | error
    cost: float = 0.0       # filled * avg_price (USDT)
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# ExchangeManager
# ---------------------------------------------------------------------------


class ExchangeManager:
    def __init__(self, api_keys: dict):
        self._keys = api_keys
        self._spot: Dict[str, Any] = {}
        self._futures: Dict[str, Any] = {}
        self._init_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Lazy init
    # ------------------------------------------------------------------

    def _ensure(self, exchange: str) -> None:
        if exchange not in self._spot:
            with self._init_lock:
                if exchange not in self._spot:
                    self._init_exchange(exchange)

    def _init_exchange(self, exchange: str) -> None:
        _log.info("Initializing exchange: %s", exchange)
        ccxt_name = _CCXT_NAME.get(exchange, exchange)
        ExClass = getattr(ccxt, ccxt_name)
        keys = self._keys.get(exchange, {})

        if not keys.get("apiKey") or not keys.get("secret"):
            _log.warning(
                "API keys for %s are empty or missing — "
                "set %s_API_KEY and %s_SECRET in .env",
                exchange,
                exchange.upper(),
                exchange.upper(),
            )

        base: dict = {
            "apiKey": keys.get("apiKey", ""),
            "secret": keys.get("secret", ""),
            "enableRateLimit": True,
        }

        # OKX requires passphrase
        if exchange == "okx":
            base["password"] = keys.get("password", "")

        spot_cfg = {**base, "options": {"defaultType": "spot"}}
        fut_cfg = {**base, "options": {"defaultType": "swap"}}

        spot = ExClass(spot_cfg)
        futures = ExClass(fut_cfg)

        # Testnet / sandbox via ccxt built-in (works for Binance, Bybit)
        if keys.get("testnet"):
            _log.info("%s: enabling sandbox/testnet mode", exchange)
            spot.set_sandbox_mode(True)
            futures.set_sandbox_mode(True)

        try:
            spot.load_markets()
        except Exception as exc:
            _log.error(
                "Failed to load spot markets for %s: %s", exchange, exc, exc_info=True
            )
            raise

        try:
            futures.load_markets()
        except Exception as exc:
            _log.error(
                "Failed to load futures markets for %s: %s", exchange, exc, exc_info=True
            )
            raise

        self._spot[exchange] = spot
        self._futures[exchange] = futures
        _log.info(
            "Exchange %s initialized — spot=%d markets, futures=%d markets",
            exchange,
            len(spot.markets),
            len(futures.markets),
        )

    def get_spot(self, exchange: str) -> Any:
        self._ensure(exchange)
        return self._spot[exchange]

    def get_futures(self, exchange: str) -> Any:
        self._ensure(exchange)
        return self._futures[exchange]

    # ------------------------------------------------------------------
    # One-time futures setup (margin mode + leverage)
    # ------------------------------------------------------------------

    def setup_futures(self, exchange: str, ccxt_symbol: str) -> list[str]:
        """
        Set margin mode = cross, leverage = 1x for a symbol.
        Returns list of status strings to display to operator.
        ccxt_symbol: BASE/QUOTE format (e.g. 'THE/USDT')
        """
        fut = self.get_futures(exchange)
        fut_sym = f"{ccxt_symbol}:USDT"
        messages: list[str] = []

        # --- Margin mode ---
        try:
            fut.set_margin_mode("cross", fut_sym)
            messages.append("margin=cross ✓")
        except Exception as e:
            err = str(e)
            already = any(k in err for k in ("No need to change", "already", "110043", "-4046"))
            if already:
                messages.append("margin=cross (already set)")
            else:
                messages.append(f"margin: {err[:60]}")

        # --- Leverage ---
        try:
            if exchange == "okx":
                fut.set_leverage(1, fut_sym, {"marginMode": "cross", "posSide": "net"})
            else:
                fut.set_leverage(1, fut_sym)
            messages.append("leverage=1x ✓")
        except Exception as e:
            messages.append(f"leverage: {str(e)[:60]}")

        # --- Contract size info ---
        try:
            market = fut.market(fut_sym)
            cs = market.get("contractSize", 1)
            messages.append(f"contractSize={cs}")
        except Exception:
            pass

        return messages

    # ------------------------------------------------------------------
    # Price / amount calculation
    # ------------------------------------------------------------------

    def calc_order(
        self,
        exchange: str,
        ccxt_symbol: str,
        side: str,
        price: float,
        usdt_size: float,
    ) -> Tuple[float, float]:
        """
        Calculate (rounded_amount, rounded_price) for a limit order.
        side: 'buy' → spot leg; 'sell' → futures leg.
        """
        if side == "buy":
            ex = self.get_spot(exchange)
            sym = ccxt_symbol
        else:
            ex = self.get_futures(exchange)
            sym = f"{ccxt_symbol}:USDT"

        raw_amount = usdt_size / price
        amount = float(ex.amount_to_precision(sym, raw_amount))
        rounded_price = float(ex.price_to_precision(sym, price))
        return amount, rounded_price

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    def place_limit(
        self,
        exchange: str,
        ccxt_symbol: str,
        side: str,
        amount: float,
        price: float,
    ) -> dict:
        """
        Place a limit order.
        side='buy'  → spot limit buy  (open long)
        side='sell' → futures limit sell (open short)
        """
        if side == "buy":
            ex = self.get_spot(exchange)
            sym = ccxt_symbol
            params = {"tdMode": "cash"} if exchange == "okx" else {}
            return ex.create_limit_buy_order(sym, amount, price, params)
        else:
            ex = self.get_futures(exchange)
            sym = f"{ccxt_symbol}:USDT"
            params = {"tdMode": "cross"} if exchange == "okx" else {}
            return ex.create_limit_sell_order(sym, amount, price, params)

    def place_market_close(
        self,
        exchange: str,
        ccxt_symbol: str,
        side: str,
        amount: float,
    ) -> dict:
        """
        Close an open position with a market order.
        side='buy'  → market sell on spot  (close long)
        side='sell' → market buy on futures (close short)
        """
        if side == "buy":
            ex = self.get_spot(exchange)
            params = {"tdMode": "cash"} if exchange == "okx" else {}
            return ex.create_market_sell_order(ccxt_symbol, amount, params)
        else:
            ex = self.get_futures(exchange)
            sym = f"{ccxt_symbol}:USDT"
            params = {"tdMode": "cross"} if exchange == "okx" else {}
            return ex.create_market_buy_order(sym, amount, params)

    # ------------------------------------------------------------------
    # Order monitoring
    # ------------------------------------------------------------------

    def _fetch_single_order(
        self, exchange: str, order_id: str, symbol: str, side: str
    ) -> dict:
        """
        Fetch one order by ID, handling exchange quirks:

        Bybit UTA quirk — fetch_order() only searches v5/order/realtime
        (active orders). Once an order is filled it moves to history and
        fetch_order raises OrderNotFound.  We try fetch_open_order first;
        on OrderNotFound we fall back to fetch_closed_order (history).

        All other exchanges: plain fetch_order with optional category param.
        """
        import ccxt as _ccxt

        if side == "buy":
            ex = self.get_spot(exchange)
            sym = symbol
            params = {"category": "spot"} if exchange == "bybit" else {}
        else:
            ex = self.get_futures(exchange)
            sym = f"{symbol}:USDT"
            params = {"category": "linear"} if exchange == "bybit" else {}

        if exchange == "bybit":
            # Try active orders first (fast path)
            try:
                return ex.fetch_open_order(order_id, sym, params)
            except _ccxt.OrderNotFound:
                pass
            except Exception as exc:
                _log.warning(
                    "bybit fetch_open_order failed for %s %s (side=%s): %s — "
                    "falling back to fetch_closed_order",
                    exchange, order_id, side, exc,
                )
            # Fall back to order history (filled / cancelled orders)
            try:
                return ex.fetch_closed_order(order_id, sym, params)
            except Exception:
                raise
        else:
            return ex.fetch_order(order_id, sym, params)

    def refresh_order(self, info: "OrderInfo", ccxt_symbol: str) -> None:
        """Update OrderInfo in-place with latest exchange data."""
        if info.status in ("closed", "canceled", "error") or not info.order_id:
            return
        try:
            raw = self._fetch_single_order(
                info.exchange, info.order_id, ccxt_symbol, info.side
            )
            # raw["status"] can exist but be None — use `or` to keep previous value
            info.status = raw.get("status") or info.status
            info.filled = float(raw.get("filled") or 0)
            info.avg_price = float(raw.get("average") or 0)
            info.cost = float(raw.get("cost") or 0)
        except Exception as exc:
            _log.warning(
                "refresh_order failed for order %s (%s %s): %s",
                info.order_id, info.exchange, info.side, exc,
            )  # keep last known state

    def cancel_order(
        self, exchange: str, order_id: str, ccxt_symbol: str, side: str
    ) -> None:
        try:
            if side == "buy":
                self.get_spot(exchange).cancel_order(order_id, ccxt_symbol)
            else:
                self.get_futures(exchange).cancel_order(
                    order_id, f"{ccxt_symbol}:USDT"
                )
        except Exception as exc:
            _log.warning(
                "cancel_order failed for order %s (%s %s): %s",
                order_id, exchange, side, exc,
            )

    # ------------------------------------------------------------------
    # Ticker (for PnL)
    # ------------------------------------------------------------------

    def fetch_ticker(
        self, exchange: str, ccxt_symbol: str, side: str
    ) -> dict:
        if side == "buy":
            return self.get_spot(exchange).fetch_ticker(ccxt_symbol)
        else:
            return self.get_futures(exchange).fetch_ticker(
                f"{ccxt_symbol}:USDT"
            )

    # ------------------------------------------------------------------
    # Market info helpers
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Scanning helpers (used by recovery mode)
    # ------------------------------------------------------------------

    def fetch_open_positions(self, exchange: str) -> list[dict]:
        """
        Return all open (non-zero) futures positions on an exchange.
        Each dict is a ccxt-normalised position object.
        """
        params = {"category": "linear"} if exchange == "bybit" else {}
        try:
            positions = self.get_futures(exchange).fetch_positions(None, params)
            return [p for p in positions if abs(float(p.get("contracts") or 0)) > 0]
        except Exception as exc:
            _log.warning("fetch_open_positions failed for %s: %s", exchange, exc)
            return []

    def fetch_recent_spot_buy_orders(
        self, exchange: str, ccxt_symbol: str, since_hours: int = 48
    ) -> list[dict]:
        """
        Return open + recently filled spot buy orders for a symbol.
        Combines fetch_open_orders + fetch_closed_orders (last `since_hours` hours).
        """
        import time as _time

        params = {"category": "spot"} if exchange == "bybit" else {}
        since_ms = int((_time.time() - since_hours * 3600) * 1000)
        results: list[dict] = []

        try:
            open_orders = self.get_spot(exchange).fetch_open_orders(
                ccxt_symbol, params=params
            )
            results.extend(o for o in open_orders if o.get("side") == "buy")
        except Exception as exc:
            _log.debug("fetch_open_orders(%s, %s): %s", exchange, ccxt_symbol, exc)

        try:
            closed = self.get_spot(exchange).fetch_closed_orders(
                ccxt_symbol, since=since_ms, limit=20, params=params
            )
            results.extend(
                o for o in closed
                if o.get("side") == "buy" and float(o.get("filled") or 0) > 0
            )
        except Exception as exc:
            _log.debug("fetch_closed_orders(%s, %s): %s", exchange, ccxt_symbol, exc)

        # deduplicate by order id
        seen: set[str] = set()
        unique: list[dict] = []
        for o in results:
            oid = str(o.get("id", ""))
            if oid not in seen:
                seen.add(oid)
                unique.append(o)
        return unique

    def get_min_amount(
        self, exchange: str, ccxt_symbol: str, side: str
    ) -> Optional[float]:
        try:
            if side == "buy":
                market = self.get_spot(exchange).market(ccxt_symbol)
            else:
                market = self.get_futures(exchange).market(
                    f"{ccxt_symbol}:USDT"
                )
            return market.get("limits", {}).get("amount", {}).get("min")
        except Exception:
            return None
