"""
Тесты для spread_scanner.py

Фокус: calc_spread() — чистая функция без I/O, легко тестируется.
"""
import os
import sys
import pytest
import asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "market-data"))

import spread_scanner as ss

# Заглушки для Prometheus-метрик уже инициализированы при импорте (_PROM_AVAILABLE=False)

NOW_MS = 1_710_000_000_000  # фиксированный "сейчас"
FRESH_TS = str(NOW_MS - 100)   # 100 мс назад — свежие данные
STALE_TS  = str(NOW_MS - 10_000)  # 10 сек назад — устаревшие (STALE_THRESHOLD_MS=5000)


# ── calc_spread ───────────────────────────────────────────────────────────────

class TestCalcSpread:
    def _call(self, buy_ask="100.0", buy_qty="1.0", buy_ts=None,
              sell_bid="100.2", sell_qty="1.0", sell_ts=None,
              direction="A", symbol="BTCUSDT",
              buy_info=("binance", "spot"), sell_info=("bybit", "futures")):
        buy_ts  = buy_ts  or FRESH_TS
        sell_ts = sell_ts or FRESH_TS
        return ss.calc_spread(
            [buy_ask, buy_qty, buy_ts],
            [sell_bid, sell_qty, sell_ts],
            direction, symbol, NOW_MS,
            buy_info, sell_info,
        )

    def test_signal_above_threshold(self):
        """Спред 0.2% > MIN_SPREAD_PCT(0.1%) → сигнал."""
        result = self._call(buy_ask="100.0", sell_bid="100.2")
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["spread_pct"] == pytest.approx(0.2, abs=1e-4)

    def test_no_signal_below_threshold(self):
        """Спред 0.05% < MIN_SPREAD_PCT(0.1%) → None."""
        result = self._call(buy_ask="100.0", sell_bid="100.05")
        assert result is None

    def test_negative_spread_returns_none(self):
        """Отрицательный спред (sell < buy) → None."""
        result = self._call(buy_ask="100.5", sell_bid="100.0")
        assert result is None

    def test_equal_prices_returns_none(self):
        """Нулевой спред → None (не превышает порог)."""
        result = self._call(buy_ask="100.0", sell_bid="100.0")
        assert result is None

    def test_stale_buy_data_returns_none(self):
        """Устаревшие данные покупки → пропустить."""
        result = self._call(buy_ts=STALE_TS)
        assert result is None

    def test_stale_sell_data_returns_none(self):
        """Устаревшие данные продажи → пропустить."""
        result = self._call(sell_ts=STALE_TS)
        assert result is None

    def test_missing_buy_ask_returns_none(self):
        """Нет цены покупки → None."""
        result = self._call(buy_ask=None)
        assert result is None

    def test_missing_sell_bid_returns_none(self):
        """Нет цены продажи → None."""
        result = self._call(sell_bid=None)
        assert result is None

    def test_zero_buy_ts_returns_none(self):
        """ts_redis == '0' → данные без метки → пропустить."""
        result = self._call(buy_ts="0")
        assert result is None

    def test_zero_sell_ts_returns_none(self):
        result = self._call(sell_ts="0")
        assert result is None

    def test_missing_ts_returns_none(self):
        """None в поле ts_redis (HMGET вернул None) → пропустить."""
        result = ss.calc_spread(
            ["100.0", "1.0", None],   # ts_redis is None
            ["100.3", "1.0", None],
            "A", "BTCUSDT", NOW_MS,
            ("binance", "spot"), ("bybit", "futures"),
        )
        assert result is None

    def test_invalid_price_returns_none(self):
        """Нечисловая цена → None."""
        result = self._call(buy_ask="N/A", sell_bid="100.2")
        assert result is None

    def test_signal_fields_complete(self):
        """Сигнал содержит все обязательные поля."""
        result = self._call(buy_ask="50000.0", sell_bid="50200.0")
        assert result is not None
        required = {
            "symbol", "direction", "spread_pct", "ts_signal",
            "buy_exchange", "buy_market", "buy_ask", "buy_ask_qty",
            "sell_exchange", "sell_market", "sell_bid", "sell_bid_qty",
        }
        assert required.issubset(result.keys())

    def test_signal_ts_is_now(self):
        """ts_signal совпадает с переданным now_ms."""
        result = self._call()
        assert result["ts_signal"] == NOW_MS

    def test_spread_pct_rounded_to_4_decimals(self):
        """spread_pct округлён до 4 знаков."""
        result = self._call(buy_ask="100.0", sell_bid="100.13579")
        assert result is not None
        assert result["spread_pct"] == round(0.13579, 4)

    def test_direction_a_exchange_labels(self):
        """Direction A: buy=binance/spot, sell=bybit/futures."""
        result = self._call(direction="A", buy_info=("binance", "spot"), sell_info=("bybit", "futures"))
        assert result["buy_exchange"] == "binance"
        assert result["buy_market"] == "spot"
        assert result["sell_exchange"] == "bybit"
        assert result["sell_market"] == "futures"

    def test_direction_b_exchange_labels(self):
        """Direction B: buy=bybit/spot, sell=binance/futures."""
        result = self._call(direction="B", buy_info=("bybit", "spot"), sell_info=("binance", "futures"))
        assert result["buy_exchange"] == "bybit"
        assert result["sell_exchange"] == "binance"

    def test_empty_strings_returns_none(self):
        """Пустые строки вместо цен → None."""
        result = self._call(buy_ask="", sell_bid="")
        assert result is None

    def test_spread_at_exact_threshold(self):
        """Спред ровно на пороге: 0.1% при MIN_SPREAD_PCT=0.1 — не превышает → None."""
        result = self._call(buy_ask="100.0", sell_bid="100.1")
        # 0.1% == threshold, должен быть None (строго больше)
        assert result is None

    def test_spread_just_above_threshold(self):
        """Спред чуть выше порога → сигнал."""
        result = self._call(buy_ask="100.0", sell_bid="100.101")
        assert result is not None
        assert result["spread_pct"] > 0.1

    def test_qty_preserved_in_signal(self):
        """Объёмы из источника попадают в сигнал без изменений."""
        result = self._call(buy_ask="100.0", buy_qty="2.5", sell_bid="100.3", sell_qty="3.7")
        assert result is not None
        assert result["buy_ask_qty"] == "2.5"
        assert result["sell_bid_qty"] == "3.7"


# ── load_symbols ──────────────────────────────────────────────────────────────

class TestLoadSymbols:
    def test_load_existing_file(self, tmp_path):
        f = tmp_path / "pairs.txt"
        f.write_text("BTCUSDT\nETHUSDT\n# comment\n\nSOLUSDT\n")
        result = ss.load_symbols(str(f))
        assert result == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    def test_uppercase(self, tmp_path):
        f = tmp_path / "pairs.txt"
        f.write_text("btcusdt\nethusdt\n")
        result = ss.load_symbols(str(f))
        assert result == ["BTCUSDT", "ETHUSDT"]

    def test_dedup(self, tmp_path):
        f = tmp_path / "pairs.txt"
        f.write_text("BTCUSDT\nBTCUSDT\nETHUSDT\n")
        result = ss.load_symbols(str(f))
        assert result == ["BTCUSDT", "ETHUSDT"]

    def test_nonexistent_file_returns_empty(self):
        result = ss.load_symbols("/nonexistent/path/pairs.txt")
        assert result == []

    def test_empty_file_returns_empty(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_text("")
        result = ss.load_symbols(str(f))
        assert result == []

    def test_only_comments_returns_empty(self, tmp_path):
        f = tmp_path / "comments.txt"
        f.write_text("# comment 1\n# comment 2\n")
        result = ss.load_symbols(str(f))
        assert result == []


# ── Сигнальный Redis-ключ ─────────────────────────────────────────────────────

class TestSignalFormat:
    def test_signal_json_serializable(self):
        """Сигнал сериализуется в JSON без ошибок."""
        import orjson
        result = TestCalcSpread()._call(buy_ask="100.0", sell_bid="100.3")
        assert result is not None
        encoded = orjson.dumps(result)
        decoded = orjson.loads(encoded)
        assert decoded["symbol"] == "BTCUSDT"
        assert decoded["spread_pct"] == result["spread_pct"]

    def test_redis_key_format(self):
        """Формат ключа сигнала: sig:spread:{direction}:{symbol}."""
        direction, symbol = "A", "BTCUSDT"
        key = f"sig:spread:{direction}:{symbol}"
        assert key == "sig:spread:A:BTCUSDT"
