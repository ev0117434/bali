import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import structlog
_log = structlog.get_logger()

import bybit_spot
import bybit_futures


SNAPSHOT_MSG = {
    "topic": "tickers.BTCUSDT",
    "type": "snapshot",
    "ts": 1710412800000,
    "data": {
        "symbol": "BTCUSDT",
        "bid1Price": "67234.50",
        "bid1Size": "1.234",
        "ask1Price": "67234.80",
        "ask1Size": "0.567",
        "lastPrice": "67234.60",
    }
}

DELTA_MSG = {
    "topic": "tickers.ETHUSDT",
    "type": "delta",
    "ts": 1710412801000,
    "data": {
        "symbol": "ETHUSDT",
        "bid1Price": "3500.10",
        "bid1Size": "2.5",
        "ask1Price": "3500.20",
        "ask1Size": "1.8",
        "lastPrice": "3500.15",
    }
}


# ---------------------------------------------------------------------------
# Bybit Spot parser
# ---------------------------------------------------------------------------
class TestBybitSpotParser:
    def test_snapshot_message(self):
        result = bybit_spot.parse_message(SNAPSHOT_MSG, _log)
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["bid"] == "67234.50"
        assert result["bid_qty"] == "1.234"
        assert result["ask"] == "67234.80"
        assert result["ask_qty"] == "0.567"
        assert result["last"] == "67234.60"
        assert result["ts_exchange"] == "1710412800000"

    def test_delta_message_processed_same_as_snapshot(self):
        """Delta updates have the same field structure and must be handled."""
        result = bybit_spot.parse_message(DELTA_MSG, _log)
        assert result is not None
        assert result["symbol"] == "ETHUSDT"
        assert result["ts_exchange"] == "1710412801000"

    def test_symbol_uppercased(self):
        msg = {**SNAPSHOT_MSG, "data": {**SNAPSHOT_MSG["data"], "symbol": "btcusdt"}}
        result = bybit_spot.parse_message(msg, _log)
        assert result["symbol"] == "BTCUSDT"

    def test_subscribe_success_returns_none(self):
        msg = {"success": True, "ret_msg": "", "op": "subscribe", "conn_id": "abc"}
        result = bybit_spot.parse_message(msg, _log)
        assert result is None

    def test_subscribe_failure_returns_none(self):
        msg = {"success": False, "ret_msg": "error", "op": "subscribe"}
        result = bybit_spot.parse_message(msg, _log)
        assert result is None

    def test_ping_returns_none(self):
        """Ping from server — handled separately in read_loop, parser returns None."""
        msg = {"op": "ping"}
        result = bybit_spot.parse_message(msg, _log)
        assert result is None

    def test_pong_response_returns_none(self):
        """Server pong heartbeat — must be silently ignored."""
        msg = {"ret_msg": "pong", "op": "pong"}
        result = bybit_spot.parse_message(msg, _log)
        assert result is None

    def test_unknown_topic_returns_none(self):
        msg = {"topic": "orderbook.BTCUSDT", "data": {}}
        result = bybit_spot.parse_message(msg, _log)
        assert result is None

    def test_empty_dict_returns_none(self):
        result = bybit_spot.parse_message({}, _log)
        assert result is None

    def test_missing_data_field_returns_none(self):
        msg = {"topic": "tickers.BTCUSDT", "ts": 1234567890000}
        result = bybit_spot.parse_message(msg, _log)
        assert result is None

    def test_missing_bid_price_defaults_to_empty(self):
        """Partial delta update may not include all fields."""
        msg = {
            "topic": "tickers.SOLUSDT",
            "type": "delta",
            "ts": 1710412800000,
            "data": {"symbol": "SOLUSDT", "lastPrice": "150.0"},
        }
        result = bybit_spot.parse_message(msg, _log)
        assert result is not None
        assert result["bid"] == ""
        assert result["ask"] == ""
        assert result["last"] == "150.0"

    def test_ts_exchange_from_outer_ts(self):
        """ts_exchange comes from the outer 'ts' field, not from data."""
        result = bybit_spot.parse_message(SNAPSHOT_MSG, _log)
        assert result["ts_exchange"] == str(SNAPSHOT_MSG["ts"])

    def test_missing_outer_ts_defaults_to_zero(self):
        msg = {k: v for k, v in SNAPSHOT_MSG.items() if k != "ts"}
        result = bybit_spot.parse_message(msg, _log)
        assert result["ts_exchange"] == "0"


# ---------------------------------------------------------------------------
# Bybit Futures parser (identical logic, different module)
# ---------------------------------------------------------------------------
class TestBybitFuturesParser:
    def test_futures_snapshot(self):
        result = bybit_futures.parse_message(SNAPSHOT_MSG, _log)
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["ts_exchange"] == "1710412800000"

    def test_futures_ping_returns_none(self):
        result = bybit_futures.parse_message({"op": "ping"}, _log)
        assert result is None

    def test_futures_subscribe_returns_none(self):
        result = bybit_futures.parse_message({"op": "subscribe", "success": True}, _log)
        assert result is None

    def test_futures_non_tickers_topic_returns_none(self):
        msg = {"topic": "kline.BTCUSDT", "data": {}}
        result = bybit_futures.parse_message(msg, _log)
        assert result is None
