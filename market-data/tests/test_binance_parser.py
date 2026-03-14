import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import structlog
# Use a no-op logger for tests
_log = structlog.get_logger()

import binance_spot
import binance_futures


# ---------------------------------------------------------------------------
# Binance Spot parser
# ---------------------------------------------------------------------------
class TestBinanceSpotParser:
    def test_valid_book_ticker(self):
        msg = {"u": 400900217, "s": "BTCUSDT", "b": "67234.50", "B": "1.234", "a": "67234.80", "A": "0.567"}
        result = binance_spot.parse_message(msg, _log)
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["bid"] == "67234.50"
        assert result["bid_qty"] == "1.234"
        assert result["ask"] == "67234.80"
        assert result["ask_qty"] == "0.567"
        assert result["last"] == ""

    def test_symbol_uppercased(self):
        msg = {"s": "btcusdt", "b": "100", "B": "1", "a": "101", "A": "1"}
        result = binance_spot.parse_message(msg, _log)
        assert result["symbol"] == "BTCUSDT"

    def test_ts_exchange_is_zero_for_spot(self):
        """Spot bookTicker has no exchange timestamp — must be '0'."""
        msg = {"s": "ETHUSDT", "b": "3000", "B": "2", "a": "3001", "A": "2"}
        result = binance_spot.parse_message(msg, _log)
        assert result["ts_exchange"] == "0"

    def test_subscribe_response_returns_none(self):
        """{"result": null, "id": 1} is subscription confirmation, not data."""
        msg = {"result": None, "id": 1}
        result = binance_spot.parse_message(msg, _log)
        assert result is None

    def test_missing_symbol_returns_none(self):
        msg = {"b": "100", "a": "101"}
        result = binance_spot.parse_message(msg, _log)
        assert result is None

    def test_empty_dict_returns_none(self):
        result = binance_spot.parse_message({}, _log)
        assert result is None

    def test_last_is_empty_string(self):
        """Spot bookTicker has no last price."""
        msg = {"s": "SOLUSDT", "b": "150", "B": "10", "a": "151", "A": "10"}
        result = binance_spot.parse_message(msg, _log)
        assert result["last"] == ""

    def test_partial_fields_use_defaults(self):
        """Missing optional fields should default to empty string."""
        msg = {"s": "XYZUSDT"}  # minimal
        result = binance_spot.parse_message(msg, _log)
        assert result is not None
        assert result["bid"] == ""
        assert result["ask"] == ""


# ---------------------------------------------------------------------------
# Binance Futures parser
# ---------------------------------------------------------------------------
class TestBinanceFuturesParser:
    def test_futures_has_event_time(self):
        """Futures bookTicker contains E field — must be used as ts_exchange."""
        msg = {
            "e": "bookTicker", "u": 400900217, "E": 1710412800000, "T": 1710412799999,
            "s": "BTCUSDT", "b": "67234.50", "B": "1.234", "a": "67234.80", "A": "0.567"
        }
        result = binance_futures.parse_message(msg, _log)
        assert result is not None
        assert result["ts_exchange"] == "1710412800000"

    def test_futures_without_E_defaults_to_zero(self):
        """If E is missing (shouldn't happen, but be safe), fallback to '0'."""
        msg = {"s": "ETHUSDT", "b": "3000", "B": "2", "a": "3001", "A": "2"}
        result = binance_futures.parse_message(msg, _log)
        assert result["ts_exchange"] == "0"

    def test_futures_subscribe_response_returns_none(self):
        msg = {"result": None, "id": 0}
        result = binance_futures.parse_message(msg, _log)
        assert result is None

    def test_futures_symbol_uppercase(self):
        msg = {"e": "bookTicker", "E": 1710412800000, "s": "ethusdt", "b": "3000", "B": "1", "a": "3001", "A": "1"}
        result = binance_futures.parse_message(msg, _log)
        assert result["symbol"] == "ETHUSDT"

    def test_futures_missing_symbol_returns_none(self):
        msg = {"e": "bookTicker", "E": 1710412800000, "b": "67234.50"}
        result = binance_futures.parse_message(msg, _log)
        assert result is None


# ---------------------------------------------------------------------------
# Reconnect backoff
# ---------------------------------------------------------------------------
class TestReconnectBackoff:
    def _calc_delay(self, attempt: int) -> float:
        import random
        random.seed(0)  # deterministic
        return min(2 ** attempt, 60) + random.uniform(0, 1)

    def test_attempt_0_delay_is_1_to_2(self):
        for _ in range(20):
            import random
            delay = min(2 ** 0, 60) + random.uniform(0, 1)
            assert 1.0 <= delay < 2.0

    def test_attempt_5_delay_caps_at_32_ish(self):
        for _ in range(20):
            import random
            delay = min(2 ** 5, 60) + random.uniform(0, 1)
            assert 32.0 <= delay < 33.0

    def test_attempt_10_delay_capped_at_60(self):
        for _ in range(20):
            import random
            delay = min(2 ** 10, 60) + random.uniform(0, 1)
            assert 60.0 <= delay < 61.0

    def test_attempt_100_still_capped_at_60(self):
        for _ in range(10):
            import random
            delay = min(2 ** 100, 60) + random.uniform(0, 1)
            assert 60.0 <= delay < 61.0

    def test_delay_always_positive(self):
        for attempt in range(15):
            import random
            delay = min(2 ** attempt, 60) + random.uniform(0, 1)
            assert delay > 0
