"""Tests for price entry logic in trade_executor."""
import time
import pytest
from arb_terminal.signal_listener import Signal
from arb_terminal.trade_executor import calculate_entry_prices


def make_signal(buy_ask: float, sell_bid: float) -> Signal:
    return Signal(
        symbol="THE/USDT", raw_symbol="THEUSDT", direction="A",
        buy_exchange="binance", buy_market="spot",
        buy_ask=buy_ask, buy_ask_qty=5000,
        sell_exchange="gateio", sell_market="futures",
        sell_bid=sell_bid, sell_bid_qty=3000,
        spread_pct=(sell_bid / buy_ask - 1) * 100,
        ts_signal=int(time.time() * 1000),
    )


class TestCalculateEntryPrices:
    """Validates the price adjustment formula from development_prompt_ru.md."""

    def test_example_from_spec(self):
        """
        From spec:
            buy_ask=0.2338, sell_bid=0.2362, SPREAD_REDUCTION=0.30
            adj_buy  = 0.2338 * 1.0015 = 0.23415 (approx)
            adj_sell = 0.2362 * 0.9985 = 0.23585 (approx)
            result_spread ≈ 0.726% (= 1.026 - 0.30)
        """
        sig = make_signal(0.2338, 0.2362)
        adj_buy, adj_sell, new_spread = calculate_entry_prices(sig, 0.30)

        assert adj_buy == pytest.approx(0.2338 * 1.0015, rel=1e-6)
        assert adj_sell == pytest.approx(0.2362 * 0.9985, rel=1e-6)

        market_spread = (0.2362 / 0.2338 - 1) * 100
        # Multiplicative shift makes the result slightly different from a simple
        # subtraction; tolerance is 0.01 pp (from spec: "≈ market_spread - REDUCTION")
        assert new_spread == pytest.approx(market_spread - 0.30, abs=0.01)

    def test_buy_price_higher_than_ask(self):
        sig = make_signal(0.2338, 0.2362)
        adj_buy, _, _ = calculate_entry_prices(sig, 0.30)
        assert adj_buy > sig.buy_ask

    def test_sell_price_lower_than_bid(self):
        sig = make_signal(0.2338, 0.2362)
        _, adj_sell, _ = calculate_entry_prices(sig, 0.30)
        assert adj_sell < sig.sell_bid

    def test_zero_reduction_no_change(self):
        sig = make_signal(0.2338, 0.2362)
        adj_buy, adj_sell, new_spread = calculate_entry_prices(sig, 0.0)
        assert adj_buy == pytest.approx(sig.buy_ask, rel=1e-9)
        assert adj_sell == pytest.approx(sig.sell_bid, rel=1e-9)
        assert new_spread == pytest.approx(sig.market_spread_pct, rel=1e-6)

    def test_equal_split(self):
        """Each leg contributes exactly half of spread_reduction."""
        sig = make_signal(100.0, 102.0)
        reduction = 0.40
        half = reduction / 2
        adj_buy, adj_sell, _ = calculate_entry_prices(sig, reduction)
        expected_buy = 100.0 * (1 + half / 100)
        expected_sell = 102.0 * (1 - half / 100)
        assert adj_buy == pytest.approx(expected_buy, rel=1e-9)
        assert adj_sell == pytest.approx(expected_sell, rel=1e-9)

    def test_btc_large_price(self):
        sig = make_signal(45000.1, 45451.5)
        adj_buy, adj_sell, new_spread = calculate_entry_prices(sig, 0.30)
        market_spread = (45451.5 / 45000.1 - 1) * 100
        assert new_spread == pytest.approx(market_spread - 0.30, abs=0.01)

    def test_proportional_not_absolute(self):
        """Price shift is multiplicative, not additive."""
        sig = make_signal(1000.0, 1010.0)
        adj_buy, adj_sell, _ = calculate_entry_prices(sig, 0.30)
        # Multiplicative: adj_buy should be exactly buy_ask * (1 + 0.15/100)
        assert adj_buy == pytest.approx(1000.0 * 1.0015, rel=1e-9)
        # Not additive: 1000.0 + 0.15% of (1010-1000) = 1000 + 0.015 — different
        assert adj_buy != pytest.approx(1000.0 + 0.015, abs=0.0001)
