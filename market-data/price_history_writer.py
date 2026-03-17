#!/usr/bin/env python3
"""
Price History Writer — rolling 20-minute chunks of price data in Redis.

═══════════════════════════════════════════════════════════════════════════════
REDIS SCHEMA
═══════════════════════════════════════════════════════════════════════════════

Price data  (one Sorted Set per exchange / market / symbol / chunk):

    Key:    md:hist:{exchange}:{market}:{symbol}:{chunk_num}
    Type:   Sorted Set (ZSET)
    Score:  ts_received in milliseconds  (integer, Unix epoch ms)
    Member: JSON string — {"bid","ask","bid_qty","ask_qty","last",
                            "ts_exchange","ts_received","ts_redis"}
    TTL:    CHUNK_DURATION_SEC × (MAX_CHUNKS + 1)  =  7200 sec
            → old chunks auto-expire, no explicit DEL needed

Config  (read-only metadata written at startup):

    Key:    md:hist:config
    Type:   Hash
    Fields: chunk_duration_sec, max_chunks, chunk_ttl_sec, sources,
            key_pattern, formula, active_formula, score_field, member_format

Live price data (written by the 8 collectors, read-only for this writer):

    Key:    md:{exchange}:{market}:{symbol}
    Type:   Hash
    Fields: bid, bid_qty, ask, ask_qty, last, ts_exchange, ts_received, ts_redis

Pub/Sub channels (published by the 8 collectors, consumed here):

    Channel: md:updates:{exchange}:{market}
    Payload: {"symbol": "BTCUSDT", "key": "md:binance:spot:BTCUSDT"}

═══════════════════════════════════════════════════════════════════════════════
HOW CHUNKS WORK
═══════════════════════════════════════════════════════════════════════════════

    CHUNK_DURATION_SEC = 1200   (20 minutes)
    CHUNK_DURATION_MS  = 1 200 000 ms

    chunk_num  =  ts_received_ms  //  CHUNK_DURATION_MS

Chunk N always covers the fixed time window:
    start_ms  =  N × 1 200 000
    end_ms    =  (N+1) × 1 200 000

Rolling window keeps MAX_CHUNKS = 5 most recent chunks (~100 min total):
    active chunks  =  range(current_chunk - 4,  current_chunk + 1)

Transition logic:
    • chunk_num is derived deterministically from each tick's ts_received.
    • When ts_received crosses into a new 20-min window the writer
      simply starts writing to a new ZSET key — no state machine needed.
    • The key for chunk N-5 expires automatically via TTL.
    • No cronjob, no explicit DEL, no background rotation task.

Visual example (chunk_num values are real epoch-based integers):

    md:hist:binance:spot:BTCUSDT:4217  TTL ~40 min  [T+0  … T+20 min)
    md:hist:binance:spot:BTCUSDT:4218  TTL ~60 min  [T+20 … T+40 min)
    md:hist:binance:spot:BTCUSDT:4219  TTL ~80 min  [T+40 … T+60 min)
    md:hist:binance:spot:BTCUSDT:4220  TTL ~100 min [T+60 … T+80 min)
    md:hist:binance:spot:BTCUSDT:4221  TTL ~120 min [T+80 … T+100 min)  ← current
    (chunk 4216 already expired)

    Same pattern for every exchange × market × symbol combination.

═══════════════════════════════════════════════════════════════════════════════
HOW TO READ HISTORY  (for future scripts)
═══════════════════════════════════════════════════════════════════════════════

Option A — import price_history_reader.py  (recommended):

    from price_history_reader import get_history, get_chunk_info

    history = await get_history(redis, "binance", "spot", "BTCUSDT")
    info    = get_chunk_info()   # active chunks + ISO time ranges, no Redis needed

Option B — inline snippet (zero dependencies):

    import time, orjson
    import redis.asyncio as aioredis

    CHUNK_DURATION_MS = 1_200_000   # 20 min in ms
    MAX_CHUNKS = 5

    async def get_history(redis, exchange, market, symbol):
        current = int(time.time() * 1000) // CHUNK_DURATION_MS
        active  = range(current - MAX_CHUNKS + 1, current + 1)

        pipe = redis.pipeline()
        for n in active:
            pipe.zrange(f"md:hist:{exchange}:{market}:{symbol}:{n}",
                        0, -1, withscores=True)
        responses = await pipe.execute()

        result = []
        for n, records in zip(active, responses):
            start_ms = n * CHUNK_DURATION_MS
            for member, score in records:
                entry = orjson.loads(member)
                entry["chunk_num"]      = n
                entry["chunk_start_ms"] = start_ms
                result.append(entry)
        return result   # sorted oldest → newest

Option C — read md:hist:config from Redis (runtime self-discovery):

    cfg = await redis.hgetall("md:hist:config")
    # cfg["key_pattern"]    → "md:hist:{exchange}:{market}:{symbol}:{chunk_num}"
    # cfg["formula"]        → "chunk_num = int(ts_received_ms / chunk_duration_ms)"
    # cfg["chunk_duration_sec"] → "1200"
    # cfg["max_chunks"]     → "5"
    # cfg["sources"]        → "binance:spot,binance:futures,bybit:spot,..."
"""

import asyncio
import os
import signal
import sys
import time

import orjson
import structlog

sys.path.insert(0, os.path.dirname(__file__))
from common import get_redis, now_ms, setup_logging

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
CHUNK_DURATION_SEC: int = int(os.getenv("PRICE_CHUNK_DURATION_SEC", "1200"))  # 20 min
MAX_CHUNKS: int          = int(os.getenv("PRICE_MAX_CHUNKS", "5"))
CHUNK_TTL_SEC: int       = CHUNK_DURATION_SEC * (MAX_CHUNKS + 1)              # 7200 sec
STATS_INTERVAL_SEC: int  = int(os.getenv("PRICE_STATS_INTERVAL_SEC", "60"))

CHUNK_DURATION_MS: int = CHUNK_DURATION_SEC * 1000

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

# pub/sub channel → (exchange, market)
_CHANNEL_TO_SOURCE: dict[str, tuple[str, str]] = {
    f"md:updates:{ex}:{mk}": (ex, mk) for ex, mk in SOURCES
}

# ─────────────────────────────────────────────────────────────────────────────
# Counters  (asyncio is single-threaded — no lock needed)
# ─────────────────────────────────────────────────────────────────────────────
_writes: int = 0
_errors: int = 0


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _chunk_num(ts_ms: int) -> int:
    """Chunk number for a millisecond timestamp."""
    return ts_ms // CHUNK_DURATION_MS


def _chunk_range_ms(n: int) -> tuple[int, int]:
    """(start_ms, end_ms) for chunk N."""
    start = n * CHUNK_DURATION_MS
    return start, start + CHUNK_DURATION_MS


def _ms_to_iso(ts_ms: int) -> str:
    import datetime
    return datetime.datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%dT%H:%M:%SZ")


# ─────────────────────────────────────────────────────────────────────────────
# Core write
# ─────────────────────────────────────────────────────────────────────────────
async def _write_tick(redis_client, exchange: str, market: str,
                      symbol: str, data: dict) -> None:
    """Write one price snapshot to the correct rolling chunk."""
    ts_received = int(data.get("ts_received") or 0)
    if ts_received == 0:
        ts_received = now_ms()

    n   = _chunk_num(ts_received)
    key = f"md:hist:{exchange}:{market}:{symbol}:{n}"

    member = orjson.dumps({
        "bid":         data.get("bid",         ""),
        "ask":         data.get("ask",         ""),
        "bid_qty":     data.get("bid_qty",     ""),
        "ask_qty":     data.get("ask_qty",     ""),
        "last":        data.get("last",        ""),
        "ts_exchange": data.get("ts_exchange", "0"),
        "ts_received": data.get("ts_received", "0"),
        "ts_redis":    data.get("ts_redis",    "0"),
    }).decode()

    pipe = redis_client.pipeline()
    pipe.zadd(key, {member: ts_received})
    pipe.expire(key, CHUNK_TTL_SEC)
    await pipe.execute()


# ─────────────────────────────────────────────────────────────────────────────
# Message handler  (fires as an asyncio Task — non-blocking)
# ─────────────────────────────────────────────────────────────────────────────
async def _handle_message(redis_client, exchange: str, market: str,
                          raw: str, log) -> None:
    global _writes, _errors
    try:
        payload = orjson.loads(raw)
        symbol  = payload["symbol"]
        src_key = payload["key"]           # e.g. "md:binance:spot:BTCUSDT"

        # Read current price snapshot from the live hash
        data = await redis_client.hgetall(src_key)
        if not data:
            return

        await _write_tick(redis_client, exchange, market, symbol, data)
        _writes += 1
    except Exception as exc:
        _errors += 1
        log.error("history_write_error",
                  exchange=exchange, market=market,
                  error=str(exc), exc_info=True)


# ─────────────────────────────────────────────────────────────────────────────
# md:hist:config  — schema metadata for future scripts
# ─────────────────────────────────────────────────────────────────────────────
async def _init_config(redis_client, log) -> None:
    cfg = {
        "chunk_duration_sec": str(CHUNK_DURATION_SEC),
        "max_chunks":         str(MAX_CHUNKS),
        "chunk_ttl_sec":      str(CHUNK_TTL_SEC),
        "sources":            ",".join(f"{ex}:{mk}" for ex, mk in SOURCES),
        "key_pattern":        "md:hist:{exchange}:{market}:{symbol}:{chunk_num}",
        "config_key":         "md:hist:config",
        "formula":            "chunk_num = int(ts_received_ms / chunk_duration_ms)",
        "active_formula":     "range(current_chunk - max_chunks + 1, current_chunk + 1)",
        "score_field":        "ts_received_ms",
        "member_format":      "JSON: bid, ask, bid_qty, ask_qty, last, ts_exchange, ts_received, ts_redis",
        "note": (
            f"{MAX_CHUNKS} rolling chunks × {CHUNK_DURATION_SEC // 60} min "
            f"= {MAX_CHUNKS * CHUNK_DURATION_SEC // 60} min total history; "
            f"TTL={CHUNK_TTL_SEC}s auto-expires old chunks"
        ),
    }
    await redis_client.hset("md:hist:config", mapping=cfg)
    log.info("hist_config_written",
             key="md:hist:config",
             chunk_duration_sec=CHUNK_DURATION_SEC,
             max_chunks=MAX_CHUNKS,
             total_history_min=MAX_CHUNKS * CHUNK_DURATION_SEC // 60)


# ─────────────────────────────────────────────────────────────────────────────
# Periodic stats
# ─────────────────────────────────────────────────────────────────────────────
async def _stats_loop(log, shutdown_event: asyncio.Event) -> None:
    global _writes, _errors
    while not shutdown_event.is_set():
        await asyncio.sleep(STATS_INTERVAL_SEC)
        if shutdown_event.is_set():
            break

        current = _chunk_num(now_ms())
        start_ms, end_ms = _chunk_range_ms(current)
        active = list(range(current - MAX_CHUNKS + 1, current + 1))

        log.info("history_stats",
                 writes=_writes,
                 errors=_errors,
                 current_chunk=current,
                 current_chunk_start=_ms_to_iso(start_ms),
                 current_chunk_end=_ms_to_iso(end_ms),
                 active_chunks=active)
        _writes = 0
        _errors = 0


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
async def main() -> None:
    log = setup_logging("price_history_writer")
    redis_client = get_redis()
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    await _init_config(redis_client, log)

    # Background stats logger
    asyncio.create_task(_stats_loop(log, shutdown_event))

    current_chunk = _chunk_num(now_ms())
    log.info("price_history_writer_started",
             sources=len(SOURCES),
             chunk_duration_sec=CHUNK_DURATION_SEC,
             max_chunks=MAX_CHUNKS,
             total_history_min=MAX_CHUNKS * CHUNK_DURATION_SEC // 60,
             current_chunk=current_chunk,
             current_chunk_start=_ms_to_iso(_chunk_range_ms(current_chunk)[0]),
             current_chunk_end=_ms_to_iso(_chunk_range_ms(current_chunk)[1]))

    channels = list(_CHANNEL_TO_SOURCE.keys())

    async with redis_client.pubsub() as ps:
        await ps.subscribe(*channels)

        async for msg in ps.listen():
            if shutdown_event.is_set():
                break
            if msg["type"] != "message":
                continue

            source = _CHANNEL_TO_SOURCE.get(msg["channel"])
            if source is None:
                continue
            exchange, market = source

            # Fire-and-forget — does not block the listener loop
            asyncio.create_task(
                _handle_message(redis_client, exchange, market, msg["data"], log)
            )

    # Graceful shutdown: wait for in-flight write tasks
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    if pending:
        await asyncio.wait(pending, timeout=5)

    await redis_client.aclose()
    log.info("price_history_writer_stopped")


if __name__ == "__main__":
    asyncio.run(main())
