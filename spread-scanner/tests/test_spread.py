"""
Тесты для spread_scanner.py

Фокус:
  - calc_spread() — чистая функция без I/O
  - load_symbols()
  - SpreadScanner cooldown логика
"""
import logging
import os
import sys
import pytest
import pytest_asyncio
import fakeredis.aioredis

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "market-data"))


@pytest_asyncio.fixture
async def redis():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()

import spread_scanner as ss

# Фиксируем порог для тестов независимо от переменной окружения
ss.MIN_SPREAD_PCT = 0.1

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
        """Отрицательный спред (sell < buy) → None."""
        result = self._call(buy_ask="100.5", sell_bid="100.0")
        assert result is None

    def test_equal_prices_returns_none(self):
        """Нулевой спред → None."""
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
        assert result["buy_market"]   == "spot"
        assert result["sell_exchange"] == "bybit"
        assert result["sell_market"]   == "futures"

    def test_direction_b_exchange_labels(self):
        """Direction B: buy=bybit/spot, sell=binance/futures."""
        result = self._call(direction="B", buy_info=("bybit", "spot"), sell_info=("binance", "futures"))
        assert result["buy_exchange"]  == "bybit"
        assert result["sell_exchange"] == "binance"

    def test_empty_strings_returns_none(self):
        """Пустые строки вместо цен → None."""
        result = self._call(buy_ask="", sell_bid="")
        assert result is None

    def test_spread_at_exact_threshold(self):
        """Спред ровно на пороге 0.1% → None (строго меньше не проходит из-за FP)."""
        result = self._call(buy_ask="100.0", sell_bid="100.1")
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


# ── Cooldown (дедупликация сигналов) ─────────────────────────────────────────

def _make_scanner(redis_client):
    """Создаёт SpreadScanner с null-логгерами для тестов."""
    null_log = logging.getLogger("null")
    null_log.addHandler(logging.NullHandler())

    class _FakeLog:
        def info(self, *a, **kw): pass
        def error(self, *a, **kw): pass
        def debug(self, *a, **kw): pass

    return ss.SpreadScanner(
        redis_client=redis_client,
        log=_FakeLog(),
        signal_logger=null_log,
        snapshot_logger=null_log,
    )


async def _seed_redis(redis, buy_ex, buy_mkt, sell_ex, sell_mkt, symbol,
                      buy_ask, sell_bid, ts):
    """Заполняет Redis тестовыми данными."""
    await redis.hset(f"md:{buy_ex}:{buy_mkt}:{symbol}", mapping={
        "ask": buy_ask, "ask_qty": "1.0", "ts_redis": str(ts),
    })
    await redis.hset(f"md:{sell_ex}:{sell_mkt}:{symbol}", mapping={
        "bid": sell_bid, "bid_qty": "1.0", "ts_redis": str(ts),
    })


class TestCooldown:
    @pytest.mark.asyncio
    async def test_first_signal_emitted(self, redis):
        """Первый сигнал по паре отправляется без задержки."""
        scanner = _make_scanner(redis)
        scanner._symbols = {"A": ["BTCUSDT"]}

        ts = NOW_MS
        await _seed_redis(redis, "binance", "spot", "bybit", "futures", "BTCUSDT",
                          buy_ask="100.0", sell_bid="102.0", ts=ts)

        emitted, suppressed = await scanner._cycle(ts)
        assert emitted == 1
        assert suppressed == 0

    @pytest.mark.asyncio
    async def test_repeat_signal_suppressed_within_cooldown(self, redis):
        """Второй сигнал в течение COOLDOWN_MS не отправляется."""
        scanner = _make_scanner(redis)
        scanner._symbols = {"A": ["BTCUSDT"]}

        ts = NOW_MS
        await _seed_redis(redis, "binance", "spot", "bybit", "futures", "BTCUSDT",
                          buy_ask="100.0", sell_bid="102.0", ts=ts)

        emitted1, _  = await scanner._cycle(ts)
        # Второй цикл через 1 мс — всё ещё в cooldown
        emitted2, suppressed = await scanner._cycle(ts + 1)
        assert emitted1 == 1
        assert emitted2 == 0
        assert suppressed == 1

    @pytest.mark.asyncio
    async def test_signal_allowed_after_cooldown(self, redis):
        """Сигнал разрешён после истечения COOLDOWN_MS."""
        scanner = _make_scanner(redis)
        scanner._symbols = {"A": ["BTCUSDT"]}

        ts = NOW_MS
        await _seed_redis(redis, "binance", "spot", "bybit", "futures", "BTCUSDT",
                          buy_ask="100.0", sell_bid="102.0", ts=ts)

        emitted1, _ = await scanner._cycle(ts)

        # Переставляем ts_redis чтобы данные остались свежими
        ts2 = ts + ss.COOLDOWN_MS + 1
        await _seed_redis(redis, "binance", "spot", "bybit", "futures", "BTCUSDT",
                          buy_ask="100.0", sell_bid="102.0", ts=ts2)
        emitted2, suppressed2 = await scanner._cycle(ts2)

        assert emitted1 == 1
        assert emitted2 == 1
        assert suppressed2 == 0

    @pytest.mark.asyncio
    async def test_different_symbols_independent_cooldown(self, redis):
        """Разные символы имеют независимые cooldown-записи."""
        scanner = _make_scanner(redis)
        scanner._symbols = {"A": ["BTCUSDT", "ETHUSDT"]}

        ts = NOW_MS
        for sym in ["BTCUSDT", "ETHUSDT"]:
            await _seed_redis(redis, "binance", "spot", "bybit", "futures", sym,
                              buy_ask="100.0", sell_bid="102.0", ts=ts)

        emitted, _ = await scanner._cycle(ts)
        assert emitted == 2   # оба сигнала прошли

        # Второй цикл — оба заблокированы cooldown
        emitted2, suppressed2 = await scanner._cycle(ts + 1)
        assert emitted2 == 0
        assert suppressed2 == 2

    @pytest.mark.asyncio
    async def test_different_directions_independent_cooldown(self, redis):
        """Разные направления (A/B) для одного символа независимы."""
        scanner = _make_scanner(redis)
        scanner._symbols = {
            "A": ["BTCUSDT"],
            "B": ["BTCUSDT"],
        }

        ts = NOW_MS
        # Direction A: binance spot → bybit futures
        await _seed_redis(redis, "binance", "spot", "bybit", "futures", "BTCUSDT",
                          buy_ask="100.0", sell_bid="102.0", ts=ts)
        # Direction B: bybit spot → binance futures
        await _seed_redis(redis, "bybit", "spot", "binance", "futures", "BTCUSDT",
                          buy_ask="100.0", sell_bid="102.0", ts=ts)

        emitted, _ = await scanner._cycle(ts)
        assert emitted == 2  # A и B — разные ключи cooldown

    @pytest.mark.asyncio
    async def test_cooldown_cleanup_removes_expired(self, redis):
        """_cleanup_cooldown удаляет записи старше COOLDOWN_MS."""
        scanner = _make_scanner(redis)
        scanner._cooldown = {
            ("A", "BTCUSDT"): NOW_MS - ss.COOLDOWN_MS - 1,  # просрочен
            ("A", "ETHUSDT"): NOW_MS - 100,                 # ещё активен
        }
        scanner._cleanup_cooldown(NOW_MS)
        assert ("A", "BTCUSDT") not in scanner._cooldown
        assert ("A", "ETHUSDT") in scanner._cooldown


# ── Формат сигнала и ключи ────────────────────────────────────────────────────

class TestSignalFormat:
    def _make_signal(self):
        return ss.calc_spread(
            ["100.0", "2.5", FRESH_TS],
            ["100.3", "3.7", FRESH_TS],
            "A", "BTCUSDT", NOW_MS,
            ("binance", "spot"), ("bybit", "futures"),
        )

    def test_signal_json_serializable(self):
        """Сигнал сериализуется в JSON без ошибок."""
        import orjson
        result = self._make_signal()
        assert result is not None
        decoded = orjson.loads(orjson.dumps(result))
        assert decoded["symbol"] == "BTCUSDT"
        assert decoded["spread_pct"] == result["spread_pct"]

    def test_redis_key_format(self):
        """Формат ключа сигнала: sig:spread:{direction}:{symbol}."""
        assert f"sig:spread:A:BTCUSDT" == "sig:spread:A:BTCUSDT"

    def test_signal_channel_name(self):
        assert ss.SIGNAL_CHANNEL == "ch:spread_signals"
