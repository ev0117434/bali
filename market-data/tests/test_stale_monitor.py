import os
import sys
import asyncio
import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import fakeredis.aioredis
import structlog
from common import now_ms
import stale_monitor

_log = structlog.get_logger()


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture
async def redis():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


async def _write_key(redis_client, key: str, age_ms: int):
    """Helper: write a Redis hash key with ts_redis = now - age_ms."""
    ts = now_ms() - age_ms
    await redis_client.hset(key, mapping={
        "bid": "100", "bid_qty": "1", "ask": "101", "ask_qty": "1",
        "last": "", "ts_exchange": "0",
        "ts_received": str(ts),
        "ts_redis": str(ts),
    })


# ---------------------------------------------------------------------------
# scan_once
# ---------------------------------------------------------------------------
class TestScanOnce:
    @pytest.mark.asyncio
    async def test_fresh_symbol_not_stale(self, redis):
        await _write_key(redis, "md:binance:spot:BTCUSDT", age_ms=10_000)  # 10 sec old
        stale_monitor.STALE_THRESHOLD_SEC = 60
        stale, total = await stale_monitor.scan_once(redis)
        assert total == 1
        assert len(stale) == 0

    @pytest.mark.asyncio
    async def test_old_symbol_is_stale(self, redis):
        await _write_key(redis, "md:binance:spot:XYZUSDT", age_ms=90_000)  # 90 sec old
        stale_monitor.STALE_THRESHOLD_SEC = 60
        stale, total = await stale_monitor.scan_once(redis)
        assert total == 1
        assert len(stale) == 1
        assert stale[0]["key"] == "md:binance:spot:XYZUSDT"
        assert stale[0]["age_sec"] >= 90

    @pytest.mark.asyncio
    async def test_multiple_mixed(self, redis):
        await _write_key(redis, "md:binance:spot:BTCUSDT", age_ms=10_000)   # fresh
        await _write_key(redis, "md:binance:spot:XYZUSDT", age_ms=90_000)   # stale
        await _write_key(redis, "md:bybit:futures:ETHUSDT", age_ms=70_000)  # stale
        stale_monitor.STALE_THRESHOLD_SEC = 60
        stale, total = await stale_monitor.scan_once(redis)
        assert total == 3
        assert len(stale) == 2
        stale_keys = {s["key"] for s in stale}
        assert "md:binance:spot:XYZUSDT" in stale_keys
        assert "md:bybit:futures:ETHUSDT" in stale_keys
        assert "md:binance:spot:BTCUSDT" not in stale_keys

    @pytest.mark.asyncio
    async def test_key_without_ts_redis_skipped(self, redis):
        """Keys missing ts_redis (corrupted) should be silently skipped."""
        await redis.hset("md:binance:spot:BROKEN", mapping={"bid": "100"})
        stale, total = await stale_monitor.scan_once(redis)
        assert total == 1
        assert len(stale) == 0  # skipped, not counted as stale

    @pytest.mark.asyncio
    async def test_empty_redis(self, redis):
        stale, total = await stale_monitor.scan_once(redis)
        assert total == 0
        assert stale == []

    @pytest.mark.asyncio
    async def test_age_sec_correct(self, redis):
        await _write_key(redis, "md:binance:spot:AGETEST", age_ms=120_000)  # exactly 120 sec
        stale_monitor.STALE_THRESHOLD_SEC = 60
        stale, _ = await stale_monitor.scan_once(redis)
        assert len(stale) == 1
        assert stale[0]["age_sec"] >= 120


# ---------------------------------------------------------------------------
# run() — grace period and recovery
# ---------------------------------------------------------------------------
class TestStaleMonitorRun:
    @pytest.mark.asyncio
    async def test_grace_period_no_alerts(self, redis):
        """During first 2 (grace) cycles, stale_detected must NOT be logged."""
        await _write_key(redis, "md:binance:spot:STALE1", age_ms=90_000)
        stale_monitor.STALE_THRESHOLD_SEC = 60
        stale_monitor.SCAN_INTERVAL_SEC = 0

        warnings = []

        class CapturingLog:
            _context = {}
            def bind(self, **kw): return self
            def info(self, event, **kw): pass
            def warning(self, event, **kw): warnings.append(event)
            def error(self, event, **kw): pass

        shutdown_event = asyncio.Event()
        cycle_count = 0
        original_scan = stale_monitor.scan_once

        async def scan_once_limited(r):
            nonlocal cycle_count
            cycle_count += 1
            result = await original_scan(r)
            # Stop after 2 grace cycles — no cycle 3 runs
            if cycle_count >= 2:
                shutdown_event.set()
            return result

        stale_monitor.scan_once = scan_once_limited
        try:
            await stale_monitor.run(redis, shutdown_event, CapturingLog())
        finally:
            stale_monitor.scan_once = original_scan

        # Both cycles were grace cycles → no stale_detected warnings
        assert cycle_count == 2
        assert "stale_detected" not in warnings

    @pytest.mark.asyncio
    async def test_stale_recovery_logged(self, redis):
        """Symbol that was stale and becomes fresh → stale_recovered logged."""
        stale_monitor.STALE_THRESHOLD_SEC = 60
        stale_monitor.SCAN_INTERVAL_SEC = 0

        recovery_logs = []

        class CapturingLog:
            _context = {}
            def bind(self, **kw): return self
            def info(self, event, **kw):
                if event == "stale_recovered":
                    recovery_logs.append(kw.get("key"))
            def warning(self, event, **kw): pass
            def error(self, event, **kw): pass

        # Simulate: cycle 1 = stale, cycle 2 = fresh
        key = "md:binance:spot:RECOVER"
        scan_calls = 0
        original_scan = stale_monitor.scan_once
        shutdown_event = asyncio.Event()

        async def mock_scan(r):
            nonlocal scan_calls
            scan_calls += 1
            if scan_calls == 1:
                return [{"key": key, "age_sec": 90}], 1
            elif scan_calls == 2:
                shutdown_event.set()
                return [], 1
            return [], 0

        stale_monitor.scan_once = mock_scan
        try:
            await stale_monitor.run(redis, shutdown_event, CapturingLog())
        finally:
            stale_monitor.scan_once = original_scan

        assert key in recovery_logs

    @pytest.mark.asyncio
    async def test_publish_on_stale(self, redis):
        """When stale detected (post-grace), alert must be published to alerts:stale."""
        stale_monitor.STALE_THRESHOLD_SEC = 60
        stale_monitor.SCAN_INTERVAL_SEC = 0
        stale_monitor.GRACE_CYCLES = 0  # skip grace for this test

        await _write_key(redis, "md:bybit:spot:ALERTTEST", age_ms=90_000)

        # Subscribe before running
        pubsub = redis.pubsub()
        await pubsub.subscribe("alerts:stale")
        await asyncio.sleep(0.01)

        shutdown_event = asyncio.Event()
        original_scan = stale_monitor.scan_once
        calls = 0

        async def scan_once_then_stop(r):
            nonlocal calls
            calls += 1
            result = await original_scan(r)
            if calls >= 1:
                shutdown_event.set()
            return result

        stale_monitor.scan_once = scan_once_then_stop
        try:
            import structlog
            await stale_monitor.run(redis, shutdown_event, structlog.get_logger())
        finally:
            stale_monitor.scan_once = original_scan
            stale_monitor.GRACE_CYCLES = 2

        # Give pubsub a moment to deliver the message
        await asyncio.sleep(0.05)

        # Drain messages, skip subscribe confirmations
        alert_msg = None
        for _ in range(10):
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if msg and msg.get("type") == "message":
                alert_msg = msg
                break
            await asyncio.sleep(0.01)

        assert alert_msg is not None, "Expected alert message in alerts:stale channel"
        import orjson
        data = orjson.loads(alert_msg["data"])
        assert data["stale_count"] == 1
        assert data["symbols"][0]["key"] == "md:bybit:spot:ALERTTEST"
