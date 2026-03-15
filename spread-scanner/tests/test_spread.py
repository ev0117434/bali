"""
Тесты для spread_scanner.py

Фокус:
  - calc_spread() — чистая функция без I/O
  - load_symbols()
  - SpreadScanner cooldown логика
"""
import asyncio
import logging
import os
import sys
import pytest
import pytest_asyncio
import fakeredis.aioredis

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "market-data"))

import spread_scanner as ss

# Фиксируем порог для тестов независимо от переменной окружения
ss.MIN_SPREAD_PCT = 0.1


@pytest_asyncio.fixture
async def redis():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


NOW_MS    = 1_710_000_000_000
FRESH_TS  = str(NOW_MS - 100)     # 100 мс назад — свежие
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
        """Спред 0.2% > порога 0.1% → сигнал."""
        result = self._call(buy_ask="100.0", sell_bid="100.2")
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["spread_pct"] == pytest.approx(0.2, abs=1e-4)

    def test_no_signal_below_threshold(self):
        """Спред 0.05% < порога 0.1% → None."""
        result = self._call(buy_ask="100.0", sell_bid="100.05")
        assert result is None

    def test_negative_spread_returns_none(self):
        result = self._call(buy_ask="100.5", sell_bid="100.0")
        assert result is None

    def test_equal_prices_returns_none(self):
        result = self._call(buy_ask="100.0", sell_bid="100.0")
        assert result is None

    def test_stale_buy_data_returns_none(self):
        result = self._call(buy_ts=STALE_TS)
        assert result is None

    def test_stale_sell_data_returns_none(self):
        result = self._call(sell_ts=STALE_TS)
        assert result is None

    def test_missing_buy_ask_returns_none(self):
        result = self._call(buy_ask=None)
        assert result is None

    def test_missing_sell_bid_returns_none(self):
        result = self._call(sell_bid=None)
        assert result is None

    def test_zero_buy_ts_returns_none(self):
        result = self._call(buy_ts="0")
        assert result is None

    def test_zero_sell_ts_returns_none(self):
        result = self._call(sell_ts="0")
        assert result is None

    def test_missing_ts_returns_none(self):
        """None в поле ts_redis (HMGET вернул None) → пропустить."""
        result = ss.calc_spread(
            ["100.0", "1.0", None],
            ["100.3", "1.0", None],
            "A", "BTCUSDT", NOW_MS,
            ("binance", "spot"), ("bybit", "futures"),
        )
        assert result is None

    def test_invalid_price_returns_none(self):
        result = self._call(buy_ask="N/A", sell_bid="100.2")
        assert result is None

    def test_signal_fields_complete(self):
        result = self._call(buy_ask="50000.0", sell_bid="50200.0")
        assert result is not None
        required = {
            "symbol", "direction", "spread_pct", "ts_signal",
            "buy_exchange", "buy_market", "buy_ask", "buy_ask_qty",
            "sell_exchange", "sell_market", "sell_bid", "sell_bid_qty",
        }
        assert required.issubset(result.keys())

    def test_signal_ts_is_now(self):
        result = self._call()
        assert result["ts_signal"] == NOW_MS

    def test_spread_pct_rounded_to_4_decimals(self):
        result = self._call(buy_ask="100.0", sell_bid="100.13579")
        assert result is not None
        assert result["spread_pct"] == round(0.13579, 4)

    def test_direction_a_exchange_labels(self):
        result = self._call(direction="A", buy_info=("binance", "spot"),
                            sell_info=("bybit", "futures"))
        assert result["buy_exchange"]  == "binance"
        assert result["buy_market"]    == "spot"
        assert result["sell_exchange"] == "bybit"
        assert result["sell_market"]   == "futures"

    def test_direction_b_exchange_labels(self):
        result = self._call(direction="B", buy_info=("bybit", "spot"),
                            sell_info=("binance", "futures"))
        assert result["buy_exchange"]  == "bybit"
        assert result["sell_exchange"] == "binance"

    def test_empty_strings_returns_none(self):
        result = self._call(buy_ask="", sell_bid="")
        assert result is None

    def test_spread_at_exact_threshold(self):
        """0.1% ровно на пороге — не проходит из-за floating point."""
        result = self._call(buy_ask="100.0", sell_bid="100.1")
        assert result is None

    def test_spread_just_above_threshold(self):
        result = self._call(buy_ask="100.0", sell_bid="100.101")
        assert result is not None
        assert result["spread_pct"] > 0.1

    def test_qty_preserved_in_signal(self):
        result = self._call(buy_ask="100.0", buy_qty="2.5", sell_bid="100.3", sell_qty="3.7")
        assert result is not None
        assert result["buy_ask_qty"]  == "2.5"
        assert result["sell_bid_qty"] == "3.7"


# ── load_symbols ──────────────────────────────────────────────────────────────

class TestLoadSymbols:
    def test_load_existing_file(self, tmp_path):
        f = tmp_path / "pairs.txt"
        f.write_text("BTCUSDT\nETHUSDT\n# comment\n\nSOLUSDT\n")
        assert ss.load_symbols(str(f)) == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    def test_uppercase(self, tmp_path):
        f = tmp_path / "pairs.txt"
        f.write_text("btcusdt\nethusdt\n")
        assert ss.load_symbols(str(f)) == ["BTCUSDT", "ETHUSDT"]

    def test_dedup(self, tmp_path):
        f = tmp_path / "pairs.txt"
        f.write_text("BTCUSDT\nBTCUSDT\nETHUSDT\n")
        assert ss.load_symbols(str(f)) == ["BTCUSDT", "ETHUSDT"]

    def test_nonexistent_file_returns_empty(self):
        assert ss.load_symbols("/nonexistent/path/pairs.txt") == []

    def test_empty_file_returns_empty(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_text("")
        assert ss.load_symbols(str(f)) == []

    def test_only_comments_returns_empty(self, tmp_path):
        f = tmp_path / "comments.txt"
        f.write_text("# comment 1\n# comment 2\n")
        assert ss.load_symbols(str(f)) == []


# ── Cooldown ──────────────────────────────────────────────────────────────────

def _make_scanner(redis_client):
    """SpreadScanner с подавленным созданием файловых логгеров."""
    class _FakeLog:
        def info(self, *a, **kw): pass
        def error(self, *a, **kw): pass

    null = logging.getLogger("null")
    null.addHandler(logging.NullHandler())

    scanner = ss.SpreadScanner.__new__(ss.SpreadScanner)
    scanner.redis        = redis_client
    scanner.log          = _FakeLog()
    scanner.activity_log = null
    scanner.signal_log   = null
    scanner._symbols     = {}
    scanner._shutdown    = asyncio.Event()
    scanner._reload_flag = False
    scanner._cooldown    = {}
    scanner._n           = 0
    return scanner


async def _seed(redis, buy_ex, buy_mkt, sell_ex, sell_mkt, symbol, buy_ask, sell_bid, ts):
    await redis.hset(f"md:{buy_ex}:{buy_mkt}:{symbol}", mapping={
        "ask": buy_ask, "ask_qty": "1.0", "ts_redis": str(ts),
    })
    await redis.hset(f"md:{sell_ex}:{sell_mkt}:{symbol}", mapping={
        "bid": sell_bid, "bid_qty": "1.0", "ts_redis": str(ts),
    })


class TestCooldown:
    @pytest.mark.asyncio
    async def test_first_signal_written(self, redis):
        """Первый сигнал по паре записывается в signals/."""
        scanner = _make_scanner(redis)
        scanner._symbols = {"A": ["BTCUSDT"]}
        ts = NOW_MS
        await _seed(redis, "binance", "spot", "bybit", "futures", "BTCUSDT",
                    "100.0", "102.0", ts)
        pairs, written, suppressed = await scanner._cycle(ts)
        assert written == 1
        assert suppressed == 0

    @pytest.mark.asyncio
    async def test_repeat_suppressed_within_cooldown(self, redis):
        """Второй сигнал в течение cooldown не пишется в signals/."""
        scanner = _make_scanner(redis)
        scanner._symbols = {"A": ["BTCUSDT"]}
        ts = NOW_MS
        await _seed(redis, "binance", "spot", "bybit", "futures", "BTCUSDT",
                    "100.0", "102.0", ts)
        _, written1, _ = await scanner._cycle(ts)
        _, written2, suppressed = await scanner._cycle(ts + 1)
        assert written1 == 1
        assert written2 == 0
        assert suppressed == 1   # спред найден, но заблокирован cooldown

    @pytest.mark.asyncio
    async def test_signal_allowed_after_cooldown(self, redis):
        """Сигнал снова пишется после истечения cooldown."""
        scanner = _make_scanner(redis)
        scanner._symbols = {"A": ["BTCUSDT"]}
        ts = NOW_MS
        await _seed(redis, "binance", "spot", "bybit", "futures", "BTCUSDT",
                    "100.0", "102.0", ts)
        _, written1, _ = await scanner._cycle(ts)

        ts2 = ts + ss.COOLDOWN_MS + 1
        await _seed(redis, "binance", "spot", "bybit", "futures", "BTCUSDT",
                    "100.0", "102.0", ts2)
        _, written2, suppressed2 = await scanner._cycle(ts2)
        assert written1 == 1
        assert written2 == 1
        assert suppressed2 == 0

    @pytest.mark.asyncio
    async def test_scanning_continues_when_suppressed(self, redis):
        """pairs всегда > 0 даже когда сигнал заблокирован cooldown."""
        scanner = _make_scanner(redis)
        scanner._symbols = {"A": ["BTCUSDT"]}
        ts = NOW_MS
        await _seed(redis, "binance", "spot", "bybit", "futures", "BTCUSDT",
                    "100.0", "102.0", ts)
        await scanner._cycle(ts)                          # первый — пишет
        pairs, written, suppressed = await scanner._cycle(ts + 1)  # второй — блок
        assert pairs == 1          # сканирование прошло
        assert written == 0        # не записано в signals/
        assert suppressed == 1     # спред найден, cooldown

    @pytest.mark.asyncio
    async def test_different_symbols_independent(self, redis):
        """Разные символы имеют независимый cooldown."""
        scanner = _make_scanner(redis)
        scanner._symbols = {"A": ["BTCUSDT", "ETHUSDT"]}
        ts = NOW_MS
        for sym in ["BTCUSDT", "ETHUSDT"]:
            await _seed(redis, "binance", "spot", "bybit", "futures", sym,
                        "100.0", "102.0", ts)
        _, written, _ = await scanner._cycle(ts)
        assert written == 2

    @pytest.mark.asyncio
    async def test_different_directions_independent(self, redis):
        """Direction A и B для одного символа — независимые cooldown-ключи."""
        scanner = _make_scanner(redis)
        scanner._symbols = {"A": ["BTCUSDT"], "B": ["BTCUSDT"]}
        ts = NOW_MS
        await _seed(redis, "binance", "spot", "bybit", "futures", "BTCUSDT",
                    "100.0", "102.0", ts)
        await _seed(redis, "bybit", "spot", "binance", "futures", "BTCUSDT",
                    "100.0", "102.0", ts)
        _, written, _ = await scanner._cycle(ts)
        assert written == 2

    def test_cleanup_removes_expired(self):
        """_cleanup_cooldown удаляет только просроченные записи."""
        scanner = _make_scanner(None)
        scanner._cooldown = {
            ("A", "BTCUSDT"): NOW_MS - ss.COOLDOWN_MS - 1,  # просрочен
            ("A", "ETHUSDT"): NOW_MS - 100,                 # активен
        }
        scanner._cleanup_cooldown(NOW_MS)
        assert ("A", "BTCUSDT") not in scanner._cooldown
        assert ("A", "ETHUSDT") in scanner._cooldown


# ── Формат сигнала ────────────────────────────────────────────────────────────

class TestSignalFormat:
    def test_signal_json_serializable(self):
        import orjson
        result = ss.calc_spread(
            ["100.0", "2.5", FRESH_TS], ["100.3", "3.7", FRESH_TS],
            "A", "BTCUSDT", NOW_MS,
            ("binance", "spot"), ("bybit", "futures"),
        )
        assert result is not None
        decoded = orjson.loads(orjson.dumps(result))
        assert decoded["symbol"] == "BTCUSDT"

    def test_signal_channel(self):
        assert ss.SIGNAL_CHANNEL == "ch:spread_signals"

    def test_scan_interval_default(self):
        """SCAN_INTERVAL_MS по умолчанию 200 (0.2 сек)."""
        assert ss.SCAN_INTERVAL_MS == 200

    def test_cooldown_default_1h(self):
        """COOLDOWN_SEC по умолчанию 3600 (1 час)."""
        assert ss.COOLDOWN_SEC == 3600
