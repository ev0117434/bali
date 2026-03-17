"""
price_history_reader.py — helpers for reading rolling price history from Redis.

Import this module in any script that needs to query historical price data.

═══════════════════════════════════════════════════════════════════════════════
REDIS SCHEMA  (written by price_history_writer.py)
═══════════════════════════════════════════════════════════════════════════════

Price data keys:

    md:hist:{exchange}:{market}:{symbol}:{chunk_num}
        Type:   Sorted Set (ZSET)
        Score:  ts_received_ms  (Unix epoch, integer milliseconds)
        Member: JSON — {"bid","ask","bid_qty","ask_qty","last",
                         "ts_exchange","ts_received","ts_redis"}
        TTL:    7200 sec  (auto-expires; no explicit DEL needed)

Config key:

    md:hist:config
        Type:   Hash
        Fields:
            chunk_duration_sec  "1200"
            max_chunks          "5"
            chunk_ttl_sec       "7200"
            sources             "binance:spot,binance:futures,bybit:spot,..."
            key_pattern         "md:hist:{exchange}:{market}:{symbol}:{chunk_num}"
            config_key          "md:hist:config"
            formula             "chunk_num = int(ts_received_ms / chunk_duration_ms)"
            active_formula      "range(current_chunk - max_chunks + 1, current_chunk + 1)"
            score_field         "ts_received_ms"
            member_format       "JSON: bid, ask, bid_qty, ask_qty, last, ..."
            note                human-readable summary

═══════════════════════════════════════════════════════════════════════════════
HOW CHUNK NUMBERS WORK
═══════════════════════════════════════════════════════════════════════════════

    CHUNK_DURATION_MS = 1_200_000  (20 minutes × 60 s × 1000 ms)

    chunk_num  = ts_received_ms // CHUNK_DURATION_MS
    start_ms   = chunk_num × CHUNK_DURATION_MS
    end_ms     = (chunk_num + 1) × CHUNK_DURATION_MS

    At any moment there are 5 active chunks (oldest → newest):
        current = int(time.time() * 1000) // CHUNK_DURATION_MS
        active  = range(current - 4, current + 1)

    Example (2026-03-17):
        chunk 4217  →  [2026-03-17T04:40:00Z … 2026-03-17T05:00:00Z)
        chunk 4218  →  [2026-03-17T05:00:00Z … 2026-03-17T05:20:00Z)
        chunk 4219  →  [2026-03-17T05:20:00Z … 2026-03-17T05:40:00Z)
        chunk 4220  →  [2026-03-17T05:40:00Z … 2026-03-17T06:00:00Z)
        chunk 4221  →  [2026-03-17T06:00:00Z … 2026-03-17T06:20:00Z)  ← current

═══════════════════════════════════════════════════════════════════════════════
QUICK START
═══════════════════════════════════════════════════════════════════════════════

    import redis.asyncio as aioredis
    from price_history_reader import get_history, get_chunk_info, get_latest

    redis = aioredis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)

    # ── All ~100 min of history for one symbol ────────────────────────────────
    history = await get_history(redis, "binance", "spot", "BTCUSDT")
    # returns: [{"bid": "...", "ask": "...", "chunk_num": 4217,
    #             "chunk_start_ms": ..., "chunk_end_ms": ..., ...}, ...]

    # ── Only the most recent 20-min chunk ────────────────────────────────────
    recent = await get_history(redis, "binance", "spot", "BTCUSDT", n_chunks=1)

    # ── Most recent single tick ───────────────────────────────────────────────
    last = await get_latest(redis, "binance", "spot", "BTCUSDT", count=1)

    # ── Specific time range ───────────────────────────────────────────────────
    data = await get_range(redis, "bybit", "futures", "ETHUSDT",
                           start_ms=1_700_000_000_000,
                           end_ms=1_700_001_200_000)

    # ── All symbols at once (single pipeline round-trip) ─────────────────────
    bulk = await get_history_multi(redis, "binance", "spot",
                                   ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    # returns: {"BTCUSDT": [...], "ETHUSDT": [...], "SOLUSDT": [...]}

    # ── Chunk info without touching Redis ────────────────────────────────────
    info = get_chunk_info()
    # returns: {
    #   "current_chunk": 4221,
    #   "active_chunks": [4217, 4218, 4219, 4220, 4221],
    #   "chunk_duration_sec": 1200,
    #   "max_chunks": 5,
    #   "total_history_min": 100,
    #   "chunk_details": [
    #     {"chunk_num": 4217,
    #      "start_ms": 5060400000000, "end_ms": 5060401200000,
    #      "start_iso": "2026-03-17T04:40:00Z",
    #      "end_iso":   "2026-03-17T05:00:00Z"},
    #     ...
    #   ]
    # }
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

# Redis key templates
_KEY   = "md:hist:{exchange}:{market}:{symbol}:{chunk_num}"
_CFG   = "md:hist:config"


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
    Return the N most recent chunk numbers, oldest first.
    Default: all 5 active chunks → ~100 min of history.
    """
    current = current_chunk_num()
    return list(range(current - n_chunks + 1, current + 1))


def _ms_to_iso(ts_ms: int) -> str:
    import datetime
    return datetime.datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hist_key(exchange: str, market: str, symbol: str, n: int) -> str:
    return f"md:hist:{exchange}:{market}:{symbol}:{n}"


# ─────────────────────────────────────────────────────────────────────────────
# Chunk info  (pure Python, no Redis needed)
# ─────────────────────────────────────────────────────────────────────────────
def get_chunk_info(n_chunks: int = MAX_CHUNKS) -> dict[str, Any]:
    """
    Return a description of currently active chunks.
    No Redis call needed — computed from wall clock only.

    Returns:
        {
          "current_chunk":      int,          # chunk being written right now
          "active_chunks":      list[int],    # [oldest … newest], len == n_chunks
          "chunk_duration_sec": int,          # 1200
          "max_chunks":         int,          # n_chunks
          "total_history_min":  int,          # n_chunks × 20
          "chunk_details": [
            {
              "chunk_num": int,
              "start_ms":  int,   # Unix ms, inclusive
              "end_ms":    int,   # Unix ms, exclusive
              "start_iso": str,   # "2026-03-17T04:40:00Z"
              "end_iso":   str,
            },
            ...                  # one entry per active chunk, oldest first
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
# get_history — full rolling window for one symbol
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

    Uses a single pipelined request (one round-trip for all chunks).
    Returns all records sorted oldest → newest.

    Args:
        redis_client: async Redis client with decode_responses=True
        exchange:     "binance" | "bybit" | "okx" | "gate"
        market:       "spot" | "futures"
        symbol:       normalised symbol, e.g. "BTCUSDT"
        n_chunks:     how many 20-min chunks to read (1–5, default 5 = ~100 min)

    Returns:
        List of dicts, each with keys:
            bid, ask, bid_qty, ask_qty, last  — strings, as stored by collector
            ts_exchange, ts_received, ts_redis — string ms timestamps
            chunk_num      — int, which chunk this record belongs to
            chunk_start_ms — int, Unix ms start of that chunk (inclusive)
            chunk_end_ms   — int, Unix ms end of that chunk (exclusive)
    """
    chunk_nums = active_chunk_nums(n_chunks)

    pipe = redis_client.pipeline()
    for n in chunk_nums:
        pipe.zrange(_hist_key(exchange, market, symbol, n), 0, -1, withscores=True)
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
# get_latest — most recent N ticks
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
    Returns records sorted oldest → newest.
    """
    chunk_nums = list(reversed(active_chunk_nums()))
    collected: list[dict] = []

    for n in chunk_nums:
        if len(collected) >= count:
            break
        need    = count - len(collected)
        records = await redis_client.zrange(
            _hist_key(exchange, market, symbol, n), -need, -1, withscores=True
        )
        for member, score in records:
            entry: dict = orjson.loads(member)
            entry["chunk_num"] = n
            collected.insert(0, entry)

    return collected[-count:]


# ─────────────────────────────────────────────────────────────────────────────
# get_range — arbitrary time window
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

    Automatically resolves which chunks cover the requested window.
    Works across chunk boundaries — e.g. a 45-min window spanning 3 chunks.

    Args:
        start_ms: Unix timestamp in milliseconds (inclusive)
        end_ms:   Unix timestamp in milliseconds (inclusive)
    """
    first_chunk = start_ms // CHUNK_DURATION_MS
    last_chunk  = end_ms   // CHUNK_DURATION_MS
    chunk_nums  = list(range(first_chunk, last_chunk + 1))

    pipe = redis_client.pipeline()
    for n in chunk_nums:
        pipe.zrangebyscore(
            _hist_key(exchange, market, symbol, n), start_ms, end_ms, withscores=True
        )
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
# get_history_multi — bulk query for many symbols (single pipeline)
# ─────────────────────────────────────────────────────────────────────────────
async def get_history_multi(
    redis_client: aioredis.Redis,
    exchange: str,
    market: str,
    symbols: list[str],
    n_chunks: int = MAX_CHUNKS,
) -> dict[str, list[dict[str, Any]]]:
    """
    Fetch rolling history for multiple symbols in a single pipeline round-trip.

    Args:
        symbols:  list of normalised symbols, e.g. ["BTCUSDT", "ETHUSDT"]

    Returns:
        {"BTCUSDT": [records...], "ETHUSDT": [records...], ...}
        Each symbol's list is sorted oldest → newest.
    """
    chunk_nums = active_chunk_nums(n_chunks)

    pipe = redis_client.pipeline()
    order: list[tuple[str, int]] = []   # (symbol, chunk_num) in pipe order
    for symbol in symbols:
        for n in chunk_nums:
            pipe.zrange(_hist_key(exchange, market, symbol, n), 0, -1, withscores=True)
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
