import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import structlog
_log = structlog.get_logger()

import okx_spot
import okx_futures


# ---------------------------------------------------------------------------
# OKX Spot parser
# ---------------------------------------------------------------------------

SPOT_TICKER_MSG = {
    "arg": {"channel": "tickers", "instId": "BTC-USDT"},
    "data": [{
        "instId": "BTC-USDT",
        "last": "67234.60",
        "askPx": "67234.80",
        "askSz": "0.567",
        "bidPx": "67234.50",
        "bidSz": "1.234",
        "ts": "1710412800000",
    }]
}

ETH_SPOT_TICKER_MSG = {
    "arg": {"channel": "tickers", "instId": "ETH-USDT"},
    "data": [{
        "instId": "ETH-USDT",
        "last": "3500.15",
        "askPx": "3500.20",
        "askSz": "1.8",
        "bidPx": "3500.10",
        "bidSz": "2.5",
        "ts": "1710412801000",
    }]
}


class TestOkxSpotParser:
    def test_spot_ticker_message(self):
        result = okx_spot.parse_message(SPOT_TICKER_MSG, _log)
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["bid"] == "67234.50"
        assert result["bid_qty"] == "1.234"
        assert result["ask"] == "67234.80"
        assert result["ask_qty"] == "0.567"
        assert result["last"] == "67234.60"
        assert result["ts_exchange"] == "1710412800000"

    def test_eth_spot_ticker(self):
        result = okx_spot.parse_message(ETH_SPOT_TICKER_MSG, _log)
        assert result is not None
        assert result["symbol"] == "ETHUSDT"
        assert result["ts_exchange"] == "1710412801000"

    def test_symbol_normalized_from_native(self):
        """BTC-USDT instId must be normalized to BTCUSDT."""
        result = okx_spot.parse_message(SPOT_TICKER_MSG, _log)
        assert result["symbol"] == "BTCUSDT"

    def test_subscribe_event_returns_none(self):
        msg = {"event": "subscribe", "arg": {"channel": "tickers", "instId": "BTC-USDT"}}
        result = okx_spot.parse_message(msg, _log)
        assert result is None

    def test_error_event_returns_none(self):
        msg = {"event": "error", "code": "60018", "msg": "Invalid request"}
        result = okx_spot.parse_message(msg, _log)
        assert result is None

    def test_wrong_channel_returns_none(self):
        msg = {"arg": {"channel": "books", "instId": "BTC-USDT"}, "data": [{}]}
        result = okx_spot.parse_message(msg, _log)
        assert result is None

    def test_empty_data_list_returns_none(self):
        msg = {"arg": {"channel": "tickers", "instId": "BTC-USDT"}, "data": []}
        result = okx_spot.parse_message(msg, _log)
        assert result is None

    def test_missing_inst_id_returns_none(self):
        msg = {
            "arg": {"channel": "tickers", "instId": "BTC-USDT"},
            "data": [{"last": "67234.60", "askPx": "67234.80", "bidPx": "67234.50", "ts": "1234"}]
        }
        result = okx_spot.parse_message(msg, _log)
        assert result is None

    def test_empty_dict_returns_none(self):
        result = okx_spot.parse_message({}, _log)
        assert result is None

    def test_non_dict_returns_none(self):
        result = okx_spot.parse_message("ping", _log)
        assert result is None

    def test_missing_fields_default_to_empty(self):
        """Partial update may not include all price fields."""
        msg = {
            "arg": {"channel": "tickers", "instId": "SOL-USDT"},
            "data": [{"instId": "SOL-USDT", "last": "150.0", "ts": "1234567890000"}]
        }
        result = okx_spot.parse_message(msg, _log)
        assert result is not None
        assert result["symbol"] == "SOLUSDT"
        assert result["bid"] == ""
        assert result["ask"] == ""
        assert result["last"] == "150.0"

    def test_missing_ts_defaults_to_zero(self):
        msg = {
            "arg": {"channel": "tickers", "instId": "BTC-USDT"},
            "data": [{"instId": "BTC-USDT", "askPx": "100", "bidPx": "99", "last": "99.5"}]
        }
        result = okx_spot.parse_message(msg, _log)
        assert result is not None
        assert result["ts_exchange"] == "0"


class TestOkxSpotNormalization:
    def test_to_native_usdt(self):
        assert okx_spot._to_native("BTCUSDT") == "BTC-USDT"

    def test_to_native_eth_quote(self):
        assert okx_spot._to_native("BTCETH") == "BTC-ETH"

    def test_to_native_usdc(self):
        assert okx_spot._to_native("ETHUSDC") == "ETH-USDC"

    def test_normalize_spot(self):
        assert okx_spot._normalize("BTC-USDT") == "BTCUSDT"

    def test_normalize_eth(self):
        assert okx_spot._normalize("ETH-BTC") == "ETHBTC"


# ---------------------------------------------------------------------------
# OKX Futures parser
# ---------------------------------------------------------------------------

FUTURES_TICKER_MSG = {
    "arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"},
    "data": [{
        "instId": "BTC-USDT-SWAP",
        "last": "67300.00",
        "askPx": "67301.00",
        "askSz": "10",
        "bidPx": "67299.50",
        "bidSz": "15",
        "ts": "1710412900000",
    }]
}


class TestOkxFuturesParser:
    def test_futures_ticker_message(self):
        result = okx_futures.parse_message(FUTURES_TICKER_MSG, _log)
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["bid"] == "67299.50"
        assert result["ask"] == "67301.00"
        assert result["ts_exchange"] == "1710412900000"

    def test_symbol_normalized_from_swap(self):
        """BTC-USDT-SWAP → BTCUSDT."""
        result = okx_futures.parse_message(FUTURES_TICKER_MSG, _log)
        assert result["symbol"] == "BTCUSDT"

    def test_non_swap_instrument_returns_none(self):
        """Spot instrument on futures channel must be ignored."""
        msg = {
            "arg": {"channel": "tickers", "instId": "BTC-USDT"},
            "data": [{"instId": "BTC-USDT", "askPx": "100", "bidPx": "99", "ts": "1234"}]
        }
        result = okx_futures.parse_message(msg, _log)
        assert result is None

    def test_subscribe_event_returns_none(self):
        msg = {"event": "subscribe", "arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"}}
        result = okx_futures.parse_message(msg, _log)
        assert result is None

    def test_error_event_returns_none(self):
        msg = {"event": "error", "code": "60018", "msg": "Invalid request"}
        result = okx_futures.parse_message(msg, _log)
        assert result is None

    def test_wrong_channel_returns_none(self):
        msg = {"arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"}, "data": [{}]}
        result = okx_futures.parse_message(msg, _log)
        assert result is None

    def test_empty_dict_returns_none(self):
        result = okx_futures.parse_message({}, _log)
        assert result is None

    def test_non_dict_returns_none(self):
        result = okx_futures.parse_message("pong", _log)
        assert result is None

    def test_eth_futures(self):
        msg = {
            "arg": {"channel": "tickers", "instId": "ETH-USDT-SWAP"},
            "data": [{
                "instId": "ETH-USDT-SWAP",
                "askPx": "3501.00",
                "askSz": "5",
                "bidPx": "3500.50",
                "bidSz": "8",
                "last": "3500.75",
                "ts": "1710412802000",
            }]
        }
        result = okx_futures.parse_message(msg, _log)
        assert result is not None
        assert result["symbol"] == "ETHUSDT"
        assert result["ask"] == "3501.00"


class TestOkxFuturesNormalization:
    def test_to_native_usdt_swap(self):
        assert okx_futures._to_native("BTCUSDT") == "BTC-USDT-SWAP"

    def test_to_native_eth_usdt_swap(self):
        assert okx_futures._to_native("ETHUSDT") == "ETH-USDT-SWAP"

    def test_normalize_swap(self):
        assert okx_futures._normalize("BTC-USDT-SWAP") == "BTCUSDT"

    def test_normalize_eth_swap(self):
        assert okx_futures._normalize("ETH-USDT-SWAP") == "ETHUSDT"
