"""
TradeExecutor — price calculation, order placement, monitoring, closing.

All methods are synchronous.  Parallel order placement uses threading.
"""

import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .exchange_manager import ExchangeManager, OrderInfo
from .signal_listener import Signal


# ---------------------------------------------------------------------------
# Price entry logic
# ---------------------------------------------------------------------------


def calculate_entry_prices(
    signal: Signal, spread_reduction: float
) -> Tuple[float, float, float]:
    """
    Adjust buy and sell prices so the limit spread is tighter by
    `spread_reduction` percentage points (split equally between legs).

    Returns: (adjusted_buy_price, adjusted_sell_price, new_spread_pct)
    """
    half = spread_reduction / 2 / 100  # pp → fraction, then halved
    adj_buy = signal.buy_ask * (1 + half)
    adj_sell = signal.sell_bid * (1 - half)
    new_spread = (adj_sell / adj_buy - 1) * 100
    return adj_buy, adj_sell, new_spread


# ---------------------------------------------------------------------------
# TradeEntry
# ---------------------------------------------------------------------------


@dataclass
class TradeEntry:
    """All data about the opening of a single trade."""

    # Prices used
    adjusted_buy_price: float = 0.0
    adjusted_sell_price: float = 0.0

    # Amounts (after exchange rounding)
    buy_amount: float = 0.0
    sell_amount: float = 0.0

    # Order objects
    buy_order: Optional[OrderInfo] = None
    sell_order: Optional[OrderInfo] = None

    # Time to fill (seconds since order placement)
    buy_fill_time: Optional[float] = None
    sell_fill_time: Optional[float] = None

    # Close order results
    buy_close_avg: float = 0.0   # average price of close market order
    sell_close_avg: float = 0.0


# ---------------------------------------------------------------------------
# TradeResult (full lifecycle record, used for logging)
# ---------------------------------------------------------------------------


@dataclass
class TradeResult:
    signal: Signal
    position_size: float
    spread_reduction: float
    entry: TradeEntry = field(default_factory=TradeEntry)

    open_time: float = 0.0   # unix timestamp when both legs opened
    close_time: float = 0.0  # unix timestamp when positions closed

    events: List[Tuple[float, str]] = field(default_factory=list)

    def add_event(self, description: str) -> None:
        self.events.append((time.time(), description))

    # --- PnL helpers ---

    @property
    def duration_sec(self) -> float:
        if self.open_time and self.close_time:
            return self.close_time - self.open_time
        return 0.0

    def buy_pnl(self) -> float:
        """Realised PnL of spot long leg (USDT)."""
        e = self.entry
        if not e.buy_order or not e.buy_close_avg:
            return 0.0
        return (e.buy_close_avg - e.buy_order.avg_price) * e.buy_order.filled

    def sell_pnl(self) -> float:
        """Realised PnL of futures short leg (USDT)."""
        e = self.entry
        if not e.sell_order or not e.sell_close_avg:
            return 0.0
        return (e.sell_order.avg_price - e.sell_close_avg) * e.sell_order.filled

    def total_pnl(self) -> float:
        return self.buy_pnl() + self.sell_pnl()


# ---------------------------------------------------------------------------
# TradeExecutor
# ---------------------------------------------------------------------------


class TradeExecutor:
    def __init__(self, em: ExchangeManager):
        self.em = em

    # ------------------------------------------------------------------
    # Order placement (parallel)
    # ------------------------------------------------------------------

    def place_orders(
        self,
        signal: Signal,
        position_size: float,
        spread_reduction: float,
    ) -> TradeEntry:
        """
        Place both limit orders simultaneously using threads.
        Returns a TradeEntry (check .buy_order.error / .sell_order.error).
        """
        adj_buy, adj_sell, _ = calculate_entry_prices(signal, spread_reduction)

        buy_amount, buy_price = self.em.calc_order(
            signal.buy_exchange, signal.symbol, "buy", adj_buy, position_size
        )
        sell_amount, sell_price = self.em.calc_order(
            signal.sell_exchange, signal.symbol, "sell", adj_sell, position_size
        )

        entry = TradeEntry(
            adjusted_buy_price=buy_price,
            adjusted_sell_price=sell_price,
            buy_amount=buy_amount,
            sell_amount=sell_amount,
        )

        buy_result: list = [None]
        sell_result: list = [None]
        buy_error: list = [None]
        sell_error: list = [None]

        def do_buy():
            try:
                raw = self.em.place_limit(
                    signal.buy_exchange, signal.symbol, "buy", buy_amount, buy_price
                )
                buy_result[0] = OrderInfo(
                    order_id=str(raw["id"]),
                    exchange=signal.buy_exchange,
                    side="buy",
                    symbol=signal.symbol,
                    planned_price=buy_price,
                    planned_amount=buy_amount,
                    status=raw.get("status", "open"),
                    filled=float(raw.get("filled") or 0),
                    avg_price=float(raw.get("average") or 0),
                    cost=float(raw.get("cost") or 0),
                )
            except Exception as exc:
                buy_error[0] = str(exc)

        def do_sell():
            try:
                raw = self.em.place_limit(
                    signal.sell_exchange, signal.symbol, "sell", sell_amount, sell_price
                )
                sell_result[0] = OrderInfo(
                    order_id=str(raw["id"]),
                    exchange=signal.sell_exchange,
                    side="sell",
                    symbol=f"{signal.symbol}:USDT",
                    planned_price=sell_price,
                    planned_amount=sell_amount,
                    status=raw.get("status", "open"),
                    filled=float(raw.get("filled") or 0),
                    avg_price=float(raw.get("average") or 0),
                    cost=float(raw.get("cost") or 0),
                )
            except Exception as exc:
                sell_error[0] = str(exc)

        t1 = threading.Thread(target=do_buy, daemon=True)
        t2 = threading.Thread(target=do_sell, daemon=True)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Attach results (success or error placeholder)
        if buy_result[0]:
            entry.buy_order = buy_result[0]
        else:
            entry.buy_order = OrderInfo(
                order_id="",
                exchange=signal.buy_exchange,
                side="buy",
                symbol=signal.symbol,
                planned_price=buy_price,
                planned_amount=buy_amount,
                status="error",
                error=buy_error[0] or "unknown",
            )

        if sell_result[0]:
            entry.sell_order = sell_result[0]
        else:
            entry.sell_order = OrderInfo(
                order_id="",
                exchange=signal.sell_exchange,
                side="sell",
                symbol=f"{signal.symbol}:USDT",
                planned_price=sell_price,
                planned_amount=sell_amount,
                status="error",
                error=sell_error[0] or "unknown",
            )

        return entry

    # ------------------------------------------------------------------
    # Order monitoring
    # ------------------------------------------------------------------

    def refresh_entry(self, entry: TradeEntry, signal: Signal) -> None:
        """Poll both orders and update in-place."""
        if entry.buy_order:
            self.em.refresh_order(entry.buy_order, signal.symbol)
        if entry.sell_order:
            self.em.refresh_order(entry.sell_order, signal.symbol)

    def both_filled(self, entry: TradeEntry) -> bool:
        b = entry.buy_order
        s = entry.sell_order
        return (
            b is not None
            and s is not None
            and b.status == "closed"
            and s.status == "closed"
        )

    def cancel_entry(self, entry: TradeEntry, signal: Signal) -> None:
        """Cancel any open orders in the entry."""
        if entry.buy_order and entry.buy_order.status == "open":
            self.em.cancel_order(
                entry.buy_order.exchange,
                entry.buy_order.order_id,
                signal.symbol,
                "buy",
            )
        if entry.sell_order and entry.sell_order.status == "open":
            self.em.cancel_order(
                entry.sell_order.exchange,
                entry.sell_order.order_id,
                signal.symbol,
                "sell",
            )

    # ------------------------------------------------------------------
    # PnL (unrealised — from live tickers)
    # ------------------------------------------------------------------

    def fetch_pnl(
        self, entry: TradeEntry, signal: Signal
    ) -> dict:
        """
        Fetch live tickers and return PnL snapshot dict:
            {spot_bid, spot_ask, fut_bid, fut_ask,
             buy_pnl, sell_pnl, total_pnl, current_spread_pct}
        """
        result = {
            "spot_bid": 0.0, "spot_ask": 0.0,
            "fut_bid": 0.0,  "fut_ask": 0.0,
            "buy_pnl": 0.0,  "sell_pnl": 0.0,
            "total_pnl": 0.0, "current_spread_pct": 0.0,
        }
        try:
            spot_ticker = self.em.fetch_ticker(
                signal.buy_exchange, signal.symbol, "buy"
            )
            result["spot_bid"] = float(spot_ticker.get("bid") or 0)
            result["spot_ask"] = float(spot_ticker.get("ask") or 0)
        except Exception:
            pass

        try:
            fut_ticker = self.em.fetch_ticker(
                signal.sell_exchange, signal.symbol, "sell"
            )
            result["fut_bid"] = float(fut_ticker.get("bid") or 0)
            result["fut_ask"] = float(fut_ticker.get("ask") or 0)
        except Exception:
            pass

        # Unrealised PnL (using bid prices as if closing now)
        bo = entry.buy_order
        so = entry.sell_order
        if bo and bo.avg_price and result["spot_bid"]:
            result["buy_pnl"] = (result["spot_bid"] - bo.avg_price) * bo.filled

        if so and so.avg_price and result["fut_ask"]:
            result["sell_pnl"] = (so.avg_price - result["fut_ask"]) * so.filled

        result["total_pnl"] = result["buy_pnl"] + result["sell_pnl"]

        # Current spread (futures bid vs spot ask)
        if result["spot_ask"] and result["fut_bid"]:
            result["current_spread_pct"] = (
                result["fut_bid"] / result["spot_ask"] - 1
            ) * 100

        return result

    # ------------------------------------------------------------------
    # Close (parallel market orders)
    # ------------------------------------------------------------------

    def close_positions(
        self, entry: TradeEntry, signal: Signal
    ) -> Tuple[Optional[dict], Optional[dict]]:
        """
        Close both legs with market orders in parallel.
        Returns (buy_close_raw, sell_close_raw) — either may be an error dict.
        """
        buy_raw: list = [None]
        sell_raw: list = [None]

        def do_close_buy():
            bo = entry.buy_order
            if bo and bo.filled > 0:
                try:
                    buy_raw[0] = self.em.place_market_close(
                        bo.exchange, signal.symbol, "buy", bo.filled
                    )
                except Exception as exc:
                    buy_raw[0] = {"error": str(exc)}

        def do_close_sell():
            so = entry.sell_order
            if so and so.filled > 0:
                try:
                    sell_raw[0] = self.em.place_market_close(
                        so.exchange, signal.symbol, "sell", so.filled
                    )
                except Exception as exc:
                    sell_raw[0] = {"error": str(exc)}

        t1 = threading.Thread(target=do_close_buy, daemon=True)
        t2 = threading.Thread(target=do_close_sell, daemon=True)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Extract average prices for PnL
        if buy_raw[0] and "error" not in buy_raw[0]:
            entry.buy_close_avg = float(
                buy_raw[0].get("average") or buy_raw[0].get("price") or 0
            )
        if sell_raw[0] and "error" not in sell_raw[0]:
            entry.sell_close_avg = float(
                sell_raw[0].get("average") or sell_raw[0].get("price") or 0
            )

        return buy_raw[0], sell_raw[0]
