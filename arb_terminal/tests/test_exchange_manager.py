"""
Tests for ExchangeManager — uses mocked ccxt classes.

These tests cover:
  - Lazy initialization of exchange instances
  - calc_order: amount and price rounding delegation
  - place_limit: correct method called on correct instance
  - place_market_close: correct method called
  - OKX-specific tdMode params
  - refresh_order: in-place update of OrderInfo
"""
import pytest
from unittest.mock import MagicMock, call, patch

from arb_terminal.exchange_manager import ExchangeManager, OrderInfo


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


def _make_spot_mock():
    m = MagicMock()
    m.amount_to_precision.side_effect = lambda sym, amt: round(amt, 2)
    m.price_to_precision.side_effect = lambda sym, price: round(price, 4)
    return m


def _make_futures_mock():
    m = MagicMock()
    m.amount_to_precision.side_effect = lambda sym, amt: round(amt, 2)
    m.price_to_precision.side_effect = lambda sym, price: round(price, 4)
    return m


@pytest.fixture
def em_binance(monkeypatch):
    """ExchangeManager with mocked Binance spot and futures instances."""
    spot = _make_spot_mock()
    futures = _make_futures_mock()

    em = ExchangeManager({"binance": {"apiKey": "k", "secret": "s"}})
    em._spot["binance"] = spot
    em._futures["binance"] = futures
    return em, spot, futures


@pytest.fixture
def em_okx(monkeypatch):
    spot = _make_spot_mock()
    futures = _make_futures_mock()

    em = ExchangeManager({"okx": {"apiKey": "k", "secret": "s", "password": "p"}})
    em._spot["okx"] = spot
    em._futures["okx"] = futures
    return em, spot, futures


# ─────────────────────────────────────────────────────────────────────────────
# calc_order
# ─────────────────────────────────────────────────────────────────────────────

class TestCalcOrder:
    def test_buy_uses_spot(self, em_binance):
        em, spot, futures = em_binance
        amount, price = em.calc_order("binance", "THE/USDT", "buy", 0.23415, 100.0)
        spot.amount_to_precision.assert_called_once()
        spot.price_to_precision.assert_called_once()
        futures.amount_to_precision.assert_not_called()

    def test_sell_uses_futures(self, em_binance):
        em, spot, futures = em_binance
        amount, price = em.calc_order("binance", "THE/USDT", "sell", 0.23585, 100.0)
        futures.amount_to_precision.assert_called_once_with("THE/USDT:USDT", pytest.approx(100/0.23585, rel=0.01))
        spot.amount_to_precision.assert_not_called()

    def test_amount_is_usdt_div_price(self, em_binance):
        em, spot, futures = em_binance
        price = 0.25
        usdt = 100.0
        amount, _ = em.calc_order("binance", "THE/USDT", "buy", price, usdt)
        # round(100 / 0.25, 2) = 400.0
        assert amount == pytest.approx(400.0, rel=0.01)

    def test_futures_symbol_appended(self, em_binance):
        em, spot, futures = em_binance
        em.calc_order("binance", "THE/USDT", "sell", 0.24, 100.0)
        call_args = futures.amount_to_precision.call_args
        assert call_args[0][0] == "THE/USDT:USDT"


# ─────────────────────────────────────────────────────────────────────────────
# place_limit
# ─────────────────────────────────────────────────────────────────────────────

class TestPlaceLimit:
    def test_buy_calls_spot_limit_buy(self, em_binance):
        em, spot, futures = em_binance
        spot.create_limit_buy_order.return_value = {
            "id": "123", "status": "open", "filled": 0, "average": None, "cost": None
        }
        em.place_limit("binance", "THE/USDT", "buy", 427.3, 0.23415)
        spot.create_limit_buy_order.assert_called_once_with(
            "THE/USDT", 427.3, 0.23415, {}
        )
        futures.create_limit_sell_order.assert_not_called()

    def test_sell_calls_futures_limit_sell(self, em_binance):
        em, spot, futures = em_binance
        futures.create_limit_sell_order.return_value = {
            "id": "456", "status": "open", "filled": 0, "average": None, "cost": None
        }
        em.place_limit("binance", "THE/USDT", "sell", 424.0, 0.23585)
        futures.create_limit_sell_order.assert_called_once_with(
            "THE/USDT:USDT", 424.0, 0.23585, {}
        )

    def test_okx_buy_includes_tdmode(self, em_okx):
        em, spot, futures = em_okx
        spot.create_limit_buy_order.return_value = {
            "id": "789", "status": "open", "filled": 0, "average": None, "cost": None
        }
        em.place_limit("okx", "THE/USDT", "buy", 100.0, 0.24)
        call_kwargs = spot.create_limit_buy_order.call_args[0]
        params = spot.create_limit_buy_order.call_args[0][3]
        assert params == {"tdMode": "cash"}

    def test_okx_sell_includes_tdmode(self, em_okx):
        em, spot, futures = em_okx
        futures.create_limit_sell_order.return_value = {
            "id": "101", "status": "open", "filled": 0, "average": None, "cost": None
        }
        em.place_limit("okx", "THE/USDT", "sell", 100.0, 0.24)
        params = futures.create_limit_sell_order.call_args[0][3]
        assert params == {"tdMode": "cross"}


# ─────────────────────────────────────────────────────────────────────────────
# place_market_close
# ─────────────────────────────────────────────────────────────────────────────

class TestPlaceMarketClose:
    def test_buy_close_calls_market_sell_on_spot(self, em_binance):
        em, spot, futures = em_binance
        spot.create_market_sell_order.return_value = {"id": "c1"}
        em.place_market_close("binance", "THE/USDT", "buy", 427.3)
        spot.create_market_sell_order.assert_called_once_with("THE/USDT", 427.3, {})

    def test_sell_close_calls_market_buy_on_futures(self, em_binance):
        em, spot, futures = em_binance
        futures.create_market_buy_order.return_value = {"id": "c2"}
        em.place_market_close("binance", "THE/USDT", "sell", 424.0)
        futures.create_market_buy_order.assert_called_once_with("THE/USDT:USDT", 424.0, {})


# ─────────────────────────────────────────────────────────────────────────────
# refresh_order
# ─────────────────────────────────────────────────────────────────────────────

class TestRefreshOrder:
    def test_updates_fields_in_place(self, em_binance):
        em, spot, futures = em_binance
        spot.fetch_order.return_value = {
            "status": "closed",
            "filled": 427.3,
            "average": 0.23415,
            "cost": 100.07,
        }
        info = OrderInfo(
            order_id="123", exchange="binance", side="buy",
            symbol="THE/USDT", planned_price=0.23415, planned_amount=427.3,
        )
        em.refresh_order(info, "THE/USDT")
        assert info.status == "closed"
        assert info.filled == pytest.approx(427.3)
        assert info.avg_price == pytest.approx(0.23415)
        assert info.cost == pytest.approx(100.07)

    def test_skips_already_closed(self, em_binance):
        em, spot, futures = em_binance
        info = OrderInfo(
            order_id="123", exchange="binance", side="buy",
            symbol="THE/USDT", planned_price=0.23415, planned_amount=427.3,
            status="closed",
        )
        em.refresh_order(info, "THE/USDT")
        spot.fetch_order.assert_not_called()

    def test_uses_futures_for_sell(self, em_binance):
        em, spot, futures = em_binance
        futures.fetch_order.return_value = {
            "status": "open", "filled": 0, "average": None, "cost": None,
        }
        info = OrderInfo(
            order_id="456", exchange="binance", side="sell",
            symbol="THE/USDT:USDT", planned_price=0.23585, planned_amount=424.0,
        )
        em.refresh_order(info, "THE/USDT")
        futures.fetch_order.assert_called_once_with("456", "THE/USDT:USDT", {})
        spot.fetch_order.assert_not_called()

    def test_exception_preserves_last_state(self, em_binance):
        em, spot, futures = em_binance
        spot.fetch_order.side_effect = Exception("network error")
        info = OrderInfo(
            order_id="123", exchange="binance", side="buy",
            symbol="THE/USDT", planned_price=0.23415, planned_amount=427.3,
            status="open", filled=100.0,
        )
        em.refresh_order(info, "THE/USDT")
        assert info.status == "open"
        assert info.filled == 100.0

    def test_bybit_futures_fallback_to_closed_order(self):
        """Bybit UTA: fetch_open_order raises OrderNotFound → falls back to fetch_closed_order."""
        import ccxt
        spot = _make_spot_mock()
        futures = _make_futures_mock()

        futures.fetch_open_order.side_effect = ccxt.OrderNotFound("not found")
        futures.fetch_closed_order.return_value = {
            "status": "closed",
            "filled": 424.0,
            "average": 0.23585,
            "cost": 99.99,
        }

        em = ExchangeManager({"bybit": {"apiKey": "k", "secret": "s"}})
        em._spot["bybit"] = spot
        em._futures["bybit"] = futures

        info = OrderInfo(
            order_id="789", exchange="bybit", side="sell",
            symbol="THE/USDT:USDT", planned_price=0.23585, planned_amount=424.0,
        )
        em.refresh_order(info, "THE/USDT")

        assert info.status == "closed"
        assert info.filled == pytest.approx(424.0)
        futures.fetch_open_order.assert_called_once()
        futures.fetch_closed_order.assert_called_once()
