"""
Integration tests: verify Redis pipeline writes produce correct data structures.
Uses fakeredis — no real Redis required.
"""
import os
import sys
import asyncio
import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import fakeredis.aioredis
import structlog
from common import now_ms, REDIS_KEY_TTL

_log = structlog.get_logger()

# Import write_redis functions from all 4 collectors
import binance_spot
import binance_futures
import bybit_spot
import bybit_futures


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def write_binance_spot(redis_client, symbol="BTCUSDT", bid="67234.50", ask="67234.80"):
    parsed = {"symbol": symbol, "bid": bid, "bid_qty": "1.234", "ask": ask, "ask_qty": "0.567", "last": "", "ts_exchange": "0"}
    return await binance_spot.write_redis(redis_client, parsed, now_ms(), _log)

async def write_binance_futures(redis_client, symbol="ETHUSDT"):
    parsed = {"symbol": symbol, "bid": "3000.10", "bid_qty": "2.0", "ask": "3000.20", "ask_qty": "1.5", "last": "", "ts_exchange": "1710412800000"}
    return await binance_futures.write_redis(redis_client, parsed, now_ms(), _log)

async def write_bybit_spot(redis_client, symbol="SOLUSDT"):
    parsed = {"symbol": symbol, "bid": "150.10", "bid_qty": "10.0", "ask": "150.20", "ask_qty": "8.0", "last": "150.15", "ts_exchange": "1710412800000"}
    return await bybit_spot.write_redis(redis_client, parsed, now_ms(), _log)

async def write_bybit_futures(redis_client, symbol="BTCUSDT"):
    parsed = {"symbol": symbol, "bid": "67000.00", "bid_qty": "5.0", "ask": "67001.00", "ask_qty": "3.0", "last": "67000.50", "ts_exchange": "1710412800000"}
    return await bybit_futures.write_redis(redis_client, parsed, now_ms(), _log)


# ---------------------------------------------------------------------------
# Redis key format
# ---------------------------------------------------------------------------
class TestRedisKeyFormat:
    @pytest.mark.asyncio
    async def test_binance_spot_key(self, redis):
        # Reset buffer
        binance_spot._redis_buffer.clear()
        await write_binance_spot(redis)
        keys = await redis.keys("md:*")
        assert "md:binance:spot:BTCUSDT" in keys

    @pytest.mark.asyncio
    async def test_binance_futures_key(self, redis):
        binance_futures._redis_buffer.clear()
        await write_binance_futures(redis)
        keys = await redis.keys("md:*")
        assert "md:binance:futures:ETHUSDT" in keys

    @pytest.mark.asyncio
    async def test_bybit_spot_key(self, redis):
        bybit_spot._redis_buffer.clear()
        await write_bybit_spot(redis)
        keys = await redis.keys("md:*")
        assert "md:bybit:spot:SOLUSDT" in keys

    @pytest.mark.asyncio
    async def test_bybit_futures_key(self, redis):
        bybit_futures._redis_buffer.clear()
        await write_bybit_futures(redis)
        keys = await redis.keys("md:*")
        assert "md:bybit:futures:BTCUSDT" in keys

    @pytest.mark.asyncio
    async def test_symbol_always_uppercase_in_key(self, redis):
        binance_spot._redis_buffer.clear()
        parsed = {"symbol": "XRPUSDT", "bid": "1", "bid_qty": "1", "ask": "1.1", "ask_qty": "1", "last": "", "ts_exchange": "0"}
        await binance_spot.write_redis(redis, parsed, now_ms(), _log)
        assert await redis.exists("md:binance:spot:XRPUSDT")


# ---------------------------------------------------------------------------
# Redis hash fields
# ---------------------------------------------------------------------------
class TestRedisHashFields:
    REQUIRED_FIELDS = {"bid", "bid_qty", "ask", "ask_qty", "last", "ts_exchange", "ts_received", "ts_redis"}

    @pytest.mark.asyncio
    async def test_all_8_fields_present(self, redis):
        # Use bybit which always provides last price (non-empty)
        bybit_spot._redis_buffer.clear()
        await write_bybit_spot(redis)
        data = await redis.hgetall("md:bybit:spot:SOLUSDT")
        assert set(data.keys()) == self.REQUIRED_FIELDS

    @pytest.mark.asyncio
    async def test_all_fields_are_strings(self, redis):
        bybit_spot._redis_buffer.clear()
        await write_bybit_spot(redis)
        data = await redis.hgetall("md:bybit:spot:SOLUSDT")
        for field, value in data.items():
            assert isinstance(value, str), f"Field {field} should be str, got {type(value)}"

    @pytest.mark.asyncio
    async def test_timestamps_are_numeric_strings(self, redis):
        binance_futures._redis_buffer.clear()
        await write_binance_futures(redis)
        data = await redis.hgetall("md:binance:futures:ETHUSDT")
        for ts_field in ("ts_exchange", "ts_received", "ts_redis"):
            val = data[ts_field]
            assert val.isdigit() or (val.startswith("-") and val[1:].isdigit()), \
                f"{ts_field}={val!r} is not a numeric string"

    @pytest.mark.asyncio
    async def test_ts_redis_is_recent(self, redis):
        """ts_redis must be within 1 second of now."""
        bybit_futures._redis_buffer.clear()
        before = now_ms()
        await write_bybit_futures(redis)
        after = now_ms()
        data = await redis.hgetall("md:bybit:futures:BTCUSDT")
        ts_redis = int(data["ts_redis"])
        assert before <= ts_redis <= after + 100

    @pytest.mark.asyncio
    async def test_ts_received_precedes_ts_redis(self, redis):
        """Processing time: ts_redis >= ts_received always."""
        binance_spot._redis_buffer.clear()
        await write_binance_spot(redis)
        data = await redis.hgetall("md:binance:spot:BTCUSDT")
        assert int(data["ts_redis"]) >= int(data["ts_received"])

    @pytest.mark.asyncio
    async def test_bybit_has_last_price(self, redis):
        bybit_spot._redis_buffer.clear()
        await write_bybit_spot(redis)
        data = await redis.hgetall("md:bybit:spot:SOLUSDT")
        assert data["last"] == "150.15"

    @pytest.mark.asyncio
    async def test_binance_spot_has_no_last_field(self, redis):
        # Binance bookTicker never provides last price, so the field is not written to Redis
        binance_spot._redis_buffer.clear()
        await write_binance_spot(redis)
        data = await redis.hgetall("md:binance:spot:BTCUSDT")
        assert "last" not in data

    @pytest.mark.asyncio
    async def test_binance_spot_ts_exchange_is_zero(self, redis):
        binance_spot._redis_buffer.clear()
        await write_binance_spot(redis)
        data = await redis.hgetall("md:binance:spot:BTCUSDT")
        assert data["ts_exchange"] == "0"

    @pytest.mark.asyncio
    async def test_partial_update_does_not_overwrite_with_empty(self, redis):
        """Partial updates with empty fields must preserve previous Redis values."""
        bybit_futures._redis_buffer.clear()
        # First update: full data
        parsed_full = {
            "symbol": "BTCUSDT", "bid": "67000.00", "bid_qty": "5.0",
            "ask": "67001.00", "ask_qty": "3.0", "last": "67000.50",
            "ts_exchange": "1000",
        }
        await bybit_futures.write_redis(redis, parsed_full, now_ms(), _log)

        # Second update: only bid changed, ask/last are empty (partial update)
        parsed_partial = {
            "symbol": "BTCUSDT", "bid": "67005.00", "bid_qty": "4.0",
            "ask": "", "ask_qty": "", "last": "",
            "ts_exchange": "2000",
        }
        await bybit_futures.write_redis(redis, parsed_partial, now_ms(), _log)

        data = await redis.hgetall("md:bybit:futures:BTCUSDT")
        # bid updated
        assert data["bid"] == "67005.00"
        assert data["bid_qty"] == "4.0"
        # ask and last preserved from first update
        assert data["ask"] == "67001.00"
        assert data["ask_qty"] == "3.0"
        assert data["last"] == "67000.50"


# ---------------------------------------------------------------------------
# TTL
# ---------------------------------------------------------------------------
class TestRedisTTL:
    @pytest.mark.asyncio
    async def test_ttl_is_set(self, redis):
        binance_spot._redis_buffer.clear()
        await write_binance_spot(redis)
        ttl = await redis.ttl("md:binance:spot:BTCUSDT")
        assert ttl > 0, "TTL must be set (key should not be persistent)"

    @pytest.mark.asyncio
    async def test_ttl_matches_config(self, redis):
        binance_spot._redis_buffer.clear()
        await write_binance_spot(redis)
        ttl = await redis.ttl("md:binance:spot:BTCUSDT")
        assert ttl <= REDIS_KEY_TTL
        assert ttl > REDIS_KEY_TTL - 5  # freshly written


# ---------------------------------------------------------------------------
# Pub/Sub channel
# ---------------------------------------------------------------------------
class TestPubSub:
    @pytest.mark.asyncio
    async def test_publish_to_binance_spot_channel(self, redis):
        binance_spot._redis_buffer.clear()
        pubsub = redis.pubsub()
        await pubsub.subscribe("md:updates:binance:spot")
        await asyncio.sleep(0.01)
        await write_binance_spot(redis)
        await asyncio.sleep(0.05)

        msg = None
        for _ in range(10):
            m = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if m and m.get("type") == "message":
                msg = m
                break
            await asyncio.sleep(0.01)

        assert msg is not None
        assert msg["channel"] == "md:updates:binance:spot"

    @pytest.mark.asyncio
    async def test_publish_to_bybit_futures_channel(self, redis):
        bybit_futures._redis_buffer.clear()
        pubsub = redis.pubsub()
        await pubsub.subscribe("md:updates:bybit:futures")
        await asyncio.sleep(0.01)
        await write_bybit_futures(redis)
        await asyncio.sleep(0.05)

        msg = None
        for _ in range(10):
            m = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if m and m.get("type") == "message":
                msg = m
                break
            await asyncio.sleep(0.01)

        assert msg is not None
        assert msg["channel"] == "md:updates:bybit:futures"


# ---------------------------------------------------------------------------
# In-memory Redis buffer (Redis-down scenario)
# ---------------------------------------------------------------------------
class TestRedisBuffer:
    @pytest.mark.asyncio
    async def test_buffer_fills_on_redis_error(self):
        """When Redis raises, data must be buffered."""
        import unittest.mock as mock

        binance_spot._redis_buffer.clear()

        # Create a mock Redis that fails on pipeline execute
        mock_redis = mock.AsyncMock()
        mock_pipe = mock.AsyncMock()
        mock_pipe.hset = mock.Mock()
        mock_pipe.expire = mock.Mock()
        mock_pipe.publish = mock.Mock()
        mock_pipe.execute = mock.AsyncMock(side_effect=Exception("Redis down"))
        mock_redis.pipeline = mock.Mock(return_value=mock_pipe)

        parsed = {"symbol": "BTCUSDT", "bid": "100", "bid_qty": "1", "ask": "101", "ask_qty": "1", "last": "", "ts_exchange": "0"}
        result = await binance_spot.write_redis(mock_redis, parsed, now_ms(), _log)

        assert result is False
        assert len(binance_spot._redis_buffer) == 1
        assert binance_spot._redis_buffer[0]["key"] == "md:binance:spot:BTCUSDT"

    @pytest.mark.asyncio
    async def test_buffer_max_size_1000(self):
        """Buffer must not exceed maxlen=1000 — oldest entries dropped."""
        import unittest.mock as mock

        binance_spot._redis_buffer.clear()
        mock_redis = mock.AsyncMock()
        mock_pipe = mock.AsyncMock()
        mock_pipe.hset = mock.Mock()
        mock_pipe.expire = mock.Mock()
        mock_pipe.publish = mock.Mock()
        mock_pipe.execute = mock.AsyncMock(side_effect=Exception("Redis down"))
        mock_redis.pipeline = mock.Mock(return_value=mock_pipe)

        for i in range(1005):
            parsed = {"symbol": f"SYM{i:04d}", "bid": "1", "bid_qty": "1", "ask": "2", "ask_qty": "1", "last": "", "ts_exchange": "0"}
            await binance_spot.write_redis(mock_redis, parsed, now_ms(), _log)

        assert len(binance_spot._redis_buffer) == 1000

    @pytest.mark.asyncio
    async def test_buffer_flushed_on_recovery(self, redis):
        """After Redis failure, next successful write must flush the buffer."""
        binance_spot._redis_buffer.clear()

        # Pre-fill buffer with 3 stale entries
        binance_spot._redis_buffer.append({
            "key": "md:binance:spot:STALE1",
            "mapping": {"bid": "1", "bid_qty": "1", "ask": "2", "ask_qty": "1", "last": "", "ts_exchange": "0", "ts_received": "0", "ts_redis": "0"},
        })
        binance_spot._redis_buffer.append({
            "key": "md:binance:spot:STALE2",
            "mapping": {"bid": "3", "bid_qty": "1", "ask": "4", "ask_qty": "1", "last": "", "ts_exchange": "0", "ts_received": "0", "ts_redis": "0"},
        })

        assert len(binance_spot._redis_buffer) == 2

        # Now write with real (fakeredis) client — should flush buffer
        parsed = {"symbol": "BTCUSDT", "bid": "100", "bid_qty": "1", "ask": "101", "ask_qty": "1", "last": "", "ts_exchange": "0"}
        result = await binance_spot.write_redis(redis, parsed, now_ms(), _log)

        assert result is True
        assert len(binance_spot._redis_buffer) == 0
        # Both stale keys should now be in Redis
        assert await redis.exists("md:binance:spot:STALE1")
        assert await redis.exists("md:binance:spot:STALE2")
