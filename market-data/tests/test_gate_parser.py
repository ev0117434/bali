import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import structlog
_log = structlog.get_logger()

import gate_spot
import gate_futures


# ---------------------------------------------------------------------------
# Shared test messages
# ---------------------------------------------------------------------------

SPOT_UPDATE_MSG = {
    "channel": "spot.book_ticker",
    "event": "update",
    "time": 1710412800,
    "result": {
        "s": "BTC_USDT",
        "b": "67234.50",
        "B": "1.234",
        "a": "67234.80",
        "A": "0.567",
    }
}

ETH_SPOT_UPDATE_MSG = {
    "channel": "spot.book_ticker",
    "event": "update",
    "time": 1710412801,
    "result": {
        "s": "ETH_USDT",
        "b": "3500.10",
        "B": "2.5",
        "a": "3500.20",
        "A": "1.8",
    }
}

FUTURES_UPDATE_MSG = {
    "channel": "futures.book_ticker",
    "event": "update",
    "time": 1710412900,
    "result": {
        "s": "BTC_USDT",
        "b": "67299.50",
        "B": "15",
        "a": "67301.00",
        "A": "10",
    }
}


# ---------------------------------------------------------------------------
# Gate.io Spot parser
# ---------------------------------------------------------------------------

class TestGateSpotParser:
    def test_spot_update_message(self):
        result = gate_spot.parse_message(SPOT_UPDATE_MSG, _log)
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["bid"] == "67234.50"
        assert result["bid_qty"] == "1.234"
        assert result["ask"] == "67234.80"
        assert result["ask_qty"] == "0.567"
        assert result["last"] == ""
        assert result["ts_exchange"] == str(1710412800 * 1000)

    def test_eth_spot_update(self):
        result = gate_spot.parse_message(ETH_SPOT_UPDATE_MSG, _log)
        assert result is not None
        assert result["symbol"] == "ETHUSDT"
        assert result["ts_exchange"] == str(1710412801 * 1000)

    def test_symbol_normalized(self):
        """BTC_USDT → BTCUSDT."""
        result = gate_spot.parse_message(SPOT_UPDATE_MSG, _log)
        assert result["symbol"] == "BTCUSDT"

    def test_last_is_always_empty(self):
        """Gate.io book_ticker has no last price field."""
        result = gate_spot.parse_message(SPOT_UPDATE_MSG, _log)
        assert result["last"] == ""

    def test_ts_exchange_in_milliseconds(self):
        """Gate.io time is epoch seconds — must be multiplied to ms."""
        result = gate_spot.parse_message(SPOT_UPDATE_MSG, _log)
        assert result["ts_exchange"] == "1710412800000"

    def test_subscribe_ack_returns_none(self):
        msg = {
            "channel": "spot.book_ticker",
            "event": "subscribe",
            "time": 1710412800,
            "result": {"status": "success"},
        }
        result = gate_spot.parse_message(msg, _log)
        assert result is None

    def test_ping_ack_returns_none(self):
        msg = {"channel": "spot.pong", "event": "update", "time": 1710412800, "result": {}}
        result = gate_spot.parse_message(msg, _log)
        assert result is None

    def test_wrong_channel_returns_none(self):
        msg = {**SPOT_UPDATE_MSG, "channel": "spot.trades"}
        result = gate_spot.parse_message(msg, _log)
        assert result is None

    def test_futures_channel_on_spot_returns_none(self):
        """Futures message must be ignored by spot parser."""
        result = gate_spot.parse_message(FUTURES_UPDATE_MSG, _log)
        assert result is None

    def test_non_update_event_returns_none(self):
        msg = {**SPOT_UPDATE_MSG, "event": "all"}
        result = gate_spot.parse_message(msg, _log)
        assert result is None

    def test_empty_result_returns_none(self):
        msg = {**SPOT_UPDATE_MSG, "result": {}}
        result = gate_spot.parse_message(msg, _log)
        assert result is None

    def test_missing_symbol_in_result_returns_none(self):
        msg = {
            "channel": "spot.book_ticker",
            "event": "update",
            "time": 1710412800,
            "result": {"b": "100", "a": "101"},
        }
        result = gate_spot.parse_message(msg, _log)
        assert result is None

    def test_empty_dict_returns_none(self):
        result = gate_spot.parse_message({}, _log)
        assert result is None

    def test_non_dict_returns_none(self):
        result = gate_spot.parse_message("ping", _log)
        assert result is None

    def test_missing_time_defaults_to_zero_ms(self):
        msg = {k: v for k, v in SPOT_UPDATE_MSG.items() if k != "time"}
        result = gate_spot.parse_message(msg, _log)
        assert result is not None
        assert result["ts_exchange"] == "0"

    def test_partial_result_missing_bid(self):
        msg = {
            "channel": "spot.book_ticker",
            "event": "update",
            "time": 1710412800,
            "result": {"s": "SOL_USDT", "a": "150.5", "A": "10"},
        }
        result = gate_spot.parse_message(msg, _log)
        assert result is not None
        assert result["symbol"] == "SOLUSDT"
        assert result["bid"] == ""
        assert result["ask"] == "150.5"


class TestGateSpotNormalization:
    def test_to_native_usdt(self):
        assert gate_spot._to_native("BTCUSDT") == "BTC_USDT"

    def test_to_native_eth(self):
        assert gate_spot._to_native("ETHUSDT") == "ETH_USDT"

    def test_to_native_btc_quote(self):
        assert gate_spot._to_native("ETHBTC") == "ETH_BTC"

    def test_to_native_usdc(self):
        assert gate_spot._to_native("SOLUSDC") == "SOL_USDC"

    def test_normalize(self):
        assert gate_spot._normalize("BTC_USDT") == "BTCUSDT"

    def test_normalize_eth(self):
        assert gate_spot._normalize("ETH_BTC") == "ETHBTC"


# ---------------------------------------------------------------------------
# Gate.io Futures parser
# ---------------------------------------------------------------------------

class TestGateFuturesParser:
    def test_futures_update_message(self):
        result = gate_futures.parse_message(FUTURES_UPDATE_MSG, _log)
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["bid"] == "67299.50"
        assert result["bid_qty"] == "15"
        assert result["ask"] == "67301.00"
        assert result["ask_qty"] == "10"
        assert result["last"] == ""
        assert result["ts_exchange"] == str(1710412900 * 1000)

    def test_symbol_normalized(self):
        result = gate_futures.parse_message(FUTURES_UPDATE_MSG, _log)
        assert result["symbol"] == "BTCUSDT"

    def test_last_is_always_empty(self):
        result = gate_futures.parse_message(FUTURES_UPDATE_MSG, _log)
        assert result["last"] == ""

    def test_ts_exchange_in_milliseconds(self):
        result = gate_futures.parse_message(FUTURES_UPDATE_MSG, _log)
        assert result["ts_exchange"] == "1710412900000"

    def test_subscribe_ack_returns_none(self):
        msg = {
            "channel": "futures.book_ticker",
            "event": "subscribe",
            "time": 1710412900,
            "result": {"status": "success"},
        }
        result = gate_futures.parse_message(msg, _log)
        assert result is None

    def test_spot_channel_on_futures_returns_none(self):
        """Spot message must be ignored by futures parser."""
        result = gate_futures.parse_message(SPOT_UPDATE_MSG, _log)
        assert result is None

    def test_wrong_channel_returns_none(self):
        msg = {**FUTURES_UPDATE_MSG, "channel": "futures.trades"}
        result = gate_futures.parse_message(msg, _log)
        assert result is None

    def test_non_update_event_returns_none(self):
        msg = {**FUTURES_UPDATE_MSG, "event": "all"}
        result = gate_futures.parse_message(msg, _log)
        assert result is None

    def test_empty_result_returns_none(self):
        msg = {**FUTURES_UPDATE_MSG, "result": {}}
        result = gate_futures.parse_message(msg, _log)
        assert result is None

    def test_empty_dict_returns_none(self):
        result = gate_futures.parse_message({}, _log)
        assert result is None

    def test_non_dict_returns_none(self):
        result = gate_futures.parse_message(42, _log)
        assert result is None

    def test_eth_futures(self):
        msg = {
            "channel": "futures.book_ticker",
            "event": "update",
            "time": 1710412901,
            "result": {
                "s": "ETH_USDT",
                "b": "3500.50",
                "B": "8",
                "a": "3501.00",
                "A": "5",
            },
        }
        result = gate_futures.parse_message(msg, _log)
        assert result is not None
        assert result["symbol"] == "ETHUSDT"
        assert result["ask"] == "3501.00"

    def test_missing_time_defaults_to_zero_ms(self):
        msg = {k: v for k, v in FUTURES_UPDATE_MSG.items() if k != "time"}
        result = gate_futures.parse_message(msg, _log)
        assert result is not None
        assert result["ts_exchange"] == "0"


class TestGateFuturesNormalization:
    def test_to_native_usdt(self):
        assert gate_futures._to_native("BTCUSDT") == "BTC_USDT"

    def test_to_native_eth_usdt(self):
        assert gate_futures._to_native("ETHUSDT") == "ETH_USDT"

    def test_normalize_usdt(self):
        assert gate_futures._normalize("BTC_USDT") == "BTCUSDT"

    def test_normalize_eth(self):
        assert gate_futures._normalize("ETH_USDT") == "ETHUSDT"
