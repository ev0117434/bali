"""
price_history_reader.py — helpers for reading rolling price history from Redis.

Import this module in any script that needs to query historical price data.

QUICK START
───────────

    import redis.asyncio as aioredis
    from price_history_reader import get_history, get_chunk_info

    redis = aioredis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)

    # All ~100 min of history for one symbol
    history = await get_history(redis, "binance", "spot", "BTCUSDT")

    # Only the last 20 min (1 chunk)
    recent = await get_history(redis, "binance", "spot", "BTCUSDT", n_chunks=1)

    # Info about what chunks are currently active
    info = get_chunk_info()
    # → {"current_chunk": 4221, "active_chunks": [4217,4218,4219,4220,4221],
    #    "chunk_details": [{"chunk_num":4217,"start_ms":...,"start_iso":...}, ...], ...}

CONSTANTS (must match price_history_writer.py)
──────────────────────────────────────────────
    CHUNK_DURATION_SEC = 1200   (20 minutes)
    MAX_CHUNKS         = 5
    CHUNK_DURATION_MS  = 1_200_000

HOW CHUNK NUMBERS MAP TO TIME
──────────────────────────────
    chunk_num  = ts_received_ms // CHUNK_DURATION_MS
    start_ms   = chunk_num * CHUNK_DURATION_MS
    end_ms     = (chunk_num + 1) * CHUNK_DURATION_MS

    Example:
        chunk 4221 → covers [4221×1200000 ms … 4222×1200000 ms)
                          =  [2026-03-17T06:00:00Z … 2026-03-17T06:20:00Z)
"""

from __future__ import annotations

import time
from typing import Any

import orjson
import redis.asyncio as aioredis

# ─────────────────────────────────────────────────────────────────────────────
# Constants  (keep in sync with price_history_writer.py / PRICE_* env vars)
# ─────────────────────────────────────────────────────────────────────────────
CHUNK_DURATION_SEC: int = 1200       # 20 minutes
MAX_CHUNKS: int          = 5
CHUNK_DURATION_MS: int   = CHUNK_DURATION_SEC * 1000
CHUNK_TTL_SEC: int       = CHUNK_DURATION_SEC * (MAX_CHUNKS + 1)

SOURCES: list[tuple[str, str]] = [
    ("binance", "spot"),
    ("binance", "futures"),
    ("bybit",   "spot"),
    ("bybit",   "futures"),
    ("okx",     "spot"),
    ("okx",     "futures"),
    ("gate",    "spot"),
    ("gate",    "futures"),
]


# ─────────────────────────────────────────────────────────────────────────────
# Low-level helpers
# ─────────────────────────────────────────────────────────────────────────────
def current_chunk_num() -> int:
    """Chunk number for right now."""
    return int(time.time() * 1000) // CHUNK_DURATION_MS


def chunk_time_range_ms(n: int) -> tuple[int, int]:
    """Return (start_ms, end_ms) for chunk N."""
    start = n * CHUNK_DURATION_MS
    return start, start + CHUNK_DURATION_MS


def active_chunk_nums(n_chunks: int = MAX_CHUNKS) -> list[int]:
    """
    Return the N most recent chunk numbers (oldest first).
    Default: 5 chunks → ~100 minutes of history.
    """
    current = current_chunk_num()
    return list(range(current - n_chunks + 1, current + 1))


def _ms_to_iso(ts_ms: int) -> str:
    import datetime
    return datetime.datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%dT%H:%M:%SZ")


# ─────────────────────────────────────────────────────────────────────────────
# Chunk info  (no Redis needed)
# ─────────────────────────────────────────────────────────────────────────────
def get_chunk_info(n_chunks: int = MAX_CHUNKS) -> dict[str, Any]:
    """
    Return a description of currently active chunks — no Redis call needed.

    Example output:
        {
          "current_chunk": 4221,
          "active_chunks": [4217, 4218, 4219, 4220, 4221],
          "chunk_duration_sec": 1200,
          "max_chunks": 5,
          "total_history_min": 100,
          "chunk_details": [
            {"chunk_num": 4217, "start_ms": ..., "end_ms": ...,
             "start_iso": "2026-03-17T04:40:00Z", "end_iso": "2026-03-17T05:00:00Z"},
            ...
          ]
        }
    """
    nums = active_chunk_nums(n_chunks)
    details = []
    for n in nums:
        start_ms, end_ms = chunk_time_range_ms(n)
        details.append({
            "chunk_num": n,
            "start_ms":  start_ms,
            "end_ms":    end_ms,
            "start_iso": _ms_to_iso(start_ms),
            "end_iso":   _ms_to_iso(end_ms),
        })
    return {
        "current_chunk":      nums[-1],
        "active_chunks":      nums,
        "chunk_duration_sec": CHUNK_DURATION_SEC,
        "max_chunks":         n_chunks,
        "total_history_min":  n_chunks * CHUNK_DURATION_SEC // 60,
        "chunk_details":      details,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Read full history
# ─────────────────────────────────────────────────────────────────────────────
async def get_history(
    redis_client: aioredis.Redis,
    exchange: str,
    market: str,
    symbol: str,
    n_chunks: int = MAX_CHUNKS,
) -> list[dict[str, Any]]:
    """
    Fetch rolling price history for one symbol.

    Reads up to `n_chunks` × 20 min of data in a single pipelined call.
    Returns records sorted oldest → newest.

    Each record dict:
        {
          "bid", "ask", "bid_qty", "ask_qty", "last",
          "ts_exchange", "ts_received", "ts_redis",  ← strings, as stored
          "chunk_num":      int,
          "chunk_start_ms": int,
          "chunk_end_ms":   int,
        }
    """
    chunk_nums = active_chunk_nums(n_chunks)

    pipe = redis_client.pipeline()
    for n in chunk_nums:
        key = f"prices:{exchange}:{market}:{symbol}:chunk:{n}"
        pipe.zrange(key, 0, -1, withscores=True)
    responses = await pipe.execute()

    result: list[dict] = []
    for n, records in zip(chunk_nums, responses):
        start_ms, end_ms = chunk_time_range_ms(n)
        for member, score in records:
            entry: dict = orjson.loads(member)
            entry["chunk_num"]      = n
            entry["chunk_start_ms"] = start_ms
            entry["chunk_end_ms"]   = end_ms
            result.append(entry)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Read most recent N ticks
# ─────────────────────────────────────────────────────────────────────────────
async def get_latest(
    redis_client: aioredis.Redis,
    exchange: str,
    market: str,
    symbol: str,
    count: int = 1,
) -> list[dict[str, Any]]:
    """
    Return the `count` most recent price ticks for one symbol.
    Searches chunks newest → oldest until enough records are found.
    """
    chunk_nums = list(reversed(active_chunk_nums()))
    collected: list[dict] = []

    for n in chunk_nums:
        if len(collected) >= count:
            break
        key = f"prices:{exchange}:{market}:{symbol}:chunk:{n}"
        need = count - len(collected)
        records = await redis_client.zrange(key, -need, -1, withscores=True)
        for member, score in records:
            entry: dict = orjson.loads(member)
            entry["chunk_num"] = n
            collected.insert(0, entry)

    return collected[-count:]


# ─────────────────────────────────────────────────────────────────────────────
# Read a specific time range
# ─────────────────────────────────────────────────────────────────────────────
async def get_range(
    redis_client: aioredis.Redis,
    exchange: str,
    market: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    """
    Return all ticks for one symbol between start_ms and end_ms (inclusive).
    Automatically selects the relevant chunks.
    """
    first_chunk = start_ms // CHUNK_DURATION_MS
    last_chunk  = end_ms   // CHUNK_DURATION_MS
    chunk_nums  = list(range(first_chunk, last_chunk + 1))

    pipe = redis_client.pipeline()
    for n in chunk_nums:
        key = f"prices:{exchange}:{market}:{symbol}:chunk:{n}"
        pipe.zrangebyscore(key, start_ms, end_ms, withscores=True)
    responses = await pipe.execute()

    result: list[dict] = []
    for n, records in zip(chunk_nums, responses):
        start_of_chunk, _ = chunk_time_range_ms(n)
        for member, score in records:
            entry: dict = orjson.loads(member)
            entry["chunk_num"]      = n
            entry["chunk_start_ms"] = start_of_chunk
            result.append(entry)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Bulk history for multiple symbols (single pipeline)
# ─────────────────────────────────────────────────────────────────────────────
async def get_history_multi(
    redis_client: aioredis.Redis,
    exchange: str,
    market: str,
    symbols: list[str],
    n_chunks: int = MAX_CHUNKS,
) -> dict[str, list[dict[str, Any]]]:
    """
    Fetch history for multiple symbols in one pipelined round-trip.
    Returns {symbol: [records...]} dict.
    """
    chunk_nums = active_chunk_nums(n_chunks)

    pipe = redis_client.pipeline()
    order: list[tuple[str, int]] = []   # (symbol, chunk_num) in pipe order
    for symbol in symbols:
        for n in chunk_nums:
            key = f"prices:{exchange}:{market}:{symbol}:chunk:{n}"
            pipe.zrange(key, 0, -1, withscores=True)
            order.append((symbol, n))
    responses = await pipe.execute()

    result: dict[str, list[dict]] = {s: [] for s in symbols}
    for (symbol, n), records in zip(order, responses):
        start_ms, end_ms = chunk_time_range_ms(n)
        for member, score in records:
            entry: dict = orjson.loads(member)
            entry["chunk_num"]      = n
            entry["chunk_start_ms"] = start_ms
            entry["chunk_end_ms"]   = end_ms
            result[symbol].append(entry)

    return result
