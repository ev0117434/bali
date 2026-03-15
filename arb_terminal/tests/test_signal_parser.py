"""Tests for signal_listener — JSON/CSV parsing and helpers."""
import time
import pytest
from arb_terminal.signal_listener import (
    parse_signal,
    raw_symbol_to_ccxt,
    normalize_exchange,
    Signal,
)


# ─────────────────────────────────────────────────────────────────────────────
# raw_symbol_to_ccxt
# ─────────────────────────────────────────────────────────────────────────────

class TestRawSymbolToCcxt:
    def test_usdt_pair(self):
        assert raw_symbol_to_ccxt("THEUSDT") == "THE/USDT"

    def test_btc_pair(self):
        assert raw_symbol_to_ccxt("BTCUSDT") == "BTC/USDT"

    def test_eth_pair(self):
        assert raw_symbol_to_ccxt("ETHUSDT") == "ETH/USDT"

    def test_long_base(self):
        assert raw_symbol_to_ccxt("WBTCUSDT") == "WBTC/USDT"

    def test_lowercase_normalised(self):
        assert raw_symbol_to_ccxt("btcusdt") == "BTC/USDT"

    def test_unknown_returns_raw(self):
        assert raw_symbol_to_ccxt("FOOBAR") == "FOOBAR"


# ─────────────────────────────────────────────────────────────────────────────
# normalize_exchange
# ─────────────────────────────────────────────────────────────────────────────

class TestNormalizeExchange:
    def test_gate_to_gateio(self):
        assert normalize_exchange("gate") == "gateio"

    def test_gate_io_to_gateio(self):
        assert normalize_exchange("gate.io") == "gateio"

    def test_binance_passthrough(self):
        assert normalize_exchange("binance") == "binance"

    def test_bybit_passthrough(self):
        assert normalize_exchange("bybit") == "bybit"

    def test_okx_passthrough(self):
        assert normalize_exchange("okx") == "okx"

    def test_uppercase_normalised(self):
        assert normalize_exchange("Binance") == "binance"


# ─────────────────────────────────────────────────────────────────────────────
# JSON parsing
# ─────────────────────────────────────────────────────────────────────────────

TS = int(time.time() * 1000)

VALID_JSON = {
    "symbol": "THEUSDT",
    "direction": "A",
    "buy_exchange": "binance",
    "buy_market": "spot",
    "buy_ask": "0.2338",
    "buy_ask_qty": "5000",
    "sell_exchange": "gate",
    "sell_market": "futures",
    "sell_bid": "0.2362",
    "sell_bid_qty": "3000",
    "spread_pct": 1.0265,
    "ts_signal": TS,
}

import json


def _json(overrides=None) -> str:
    d = {**VALID_JSON}
    if overrides:
        d.update(overrides)
    return json.dumps(d)


class TestJsonParsing:
    def test_full_valid(self):
        sig = parse_signal(_json())
        assert sig is not None
        assert sig.symbol == "THE/USDT"
        assert sig.raw_symbol == "THEUSDT"
        assert sig.direction == "A"
        assert sig.buy_exchange == "binance"
        assert sig.buy_market == "spot"
        assert sig.buy_ask == pytest.approx(0.2338)
        assert sig.buy_ask_qty == pytest.approx(5000)
        assert sig.sell_exchange == "gateio"
        assert sig.sell_market == "futures"
        assert sig.sell_bid == pytest.approx(0.2362)
        assert sig.sell_bid_qty == pytest.approx(3000)
        assert sig.spread_pct == pytest.approx(1.0265)
        assert sig.ts_signal == TS

    def test_gate_normalised(self):
        sig = parse_signal(_json({"sell_exchange": "gate"}))
        assert sig.sell_exchange == "gateio"

    def test_gate_io_normalised(self):
        sig = parse_signal(_json({"sell_exchange": "gate.io"}))
        assert sig.sell_exchange == "gateio"

    def test_numeric_ask(self):
        sig = parse_signal(_json({"buy_ask": 0.2338}))
        assert sig.buy_ask == pytest.approx(0.2338)

    def test_missing_optional_fields(self):
        d = {k: v for k, v in VALID_JSON.items()
             if k not in ("direction", "buy_market", "sell_market", "buy_ask_qty", "sell_bid_qty")}
        sig = parse_signal(json.dumps(d))
        assert sig is not None
        assert sig.direction == "?"
        assert sig.buy_market == "spot"
        assert sig.sell_market == "futures"
        assert sig.buy_ask_qty == 0.0

    def test_missing_required_field_returns_none(self):
        d = {k: v for k, v in VALID_JSON.items() if k != "buy_ask"}
        assert parse_signal(json.dumps(d)) is None

    def test_invalid_json_falls_through_to_csv(self):
        # Not JSON at all — falls through to CSV; CSV parse also fails → None
        sig = parse_signal("not json at all")
        assert sig is None


# ─────────────────────────────────────────────────────────────────────────────
# CSV parsing
# ─────────────────────────────────────────────────────────────────────────────

class TestCsvParsing:
    def _csv(self, **kw):
        vals = dict(
            buy_ex="binance",
            sell_part="gate.THEUSDT",
            buy_ask="0.2338",
            sell_bid="0.2362",
            spread="1.0265",
            ts=str(TS),
        )
        vals.update(kw)
        return f"{vals['buy_ex']},{vals['sell_part']},{vals['buy_ask']},{vals['sell_bid']},{vals['spread']},{vals['ts']}"

    def test_basic_csv(self):
        sig = parse_signal(self._csv())
        assert sig is not None
        assert sig.symbol == "THE/USDT"
        assert sig.buy_exchange == "binance"
        assert sig.sell_exchange == "gateio"
        assert sig.buy_ask == pytest.approx(0.2338)
        assert sig.sell_bid == pytest.approx(0.2362)
        assert sig.spread_pct == pytest.approx(1.0265)

    def test_btc_csv(self):
        sig = parse_signal(self._csv(sell_part="bybit.BTCUSDT", buy_ask="45000.1", sell_bid="45200.0", spread="0.444"))
        assert sig.symbol == "BTC/USDT"
        assert sig.sell_exchange == "bybit"

    def test_direction_is_unknown(self):
        sig = parse_signal(self._csv())
        assert sig.direction == "?"

    def test_too_few_columns_returns_none(self):
        assert parse_signal("binance,gate.THEUSDT,0.2338") is None

    def test_invalid_number_returns_none(self):
        # "nan" parses as float in Python; use a truly non-numeric string
        assert parse_signal(self._csv(buy_ask="abc")) is None


# ─────────────────────────────────────────────────────────────────────────────
# Signal properties
# ─────────────────────────────────────────────────────────────────────────────

class TestSignalProperties:
    def _make(self, buy_ask=0.2338, sell_bid=0.2362, ts_offset_ms=0):
        return Signal(
            symbol="THE/USDT", raw_symbol="THEUSDT", direction="A",
            buy_exchange="binance", buy_market="spot",
            buy_ask=buy_ask, buy_ask_qty=5000,
            sell_exchange="gateio", sell_market="futures",
            sell_bid=sell_bid, sell_bid_qty=3000,
            spread_pct=1.0265,
            ts_signal=int(time.time() * 1000) - ts_offset_ms,
        )

    def test_age_sec_fresh(self):
        sig = self._make()
        assert sig.age_sec < 1.0

    def test_age_sec_old(self):
        sig = self._make(ts_offset_ms=10_000)
        assert 9 < sig.age_sec < 11

    def test_market_spread_pct(self):
        sig = self._make(buy_ask=0.2338, sell_bid=0.2362)
        expected = (0.2362 / 0.2338 - 1) * 100
        assert sig.market_spread_pct == pytest.approx(expected, rel=1e-4)
