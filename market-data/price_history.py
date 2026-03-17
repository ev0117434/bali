"""
Price History Writer

Subscribes to all 8 price update channels via Redis Pub/Sub pattern subscription.
For each update, fetches bid/ask/ts and appends it to a 20-minute chunk list.

Circular buffer logic:
  - 5 chunks of 20 minutes each = 100 minutes of history per source per symbol
  - chunk_num = unix_time // CHUNK_SECONDS   (global ever-increasing index)
  - slot      = chunk_num % MAX_CHUNKS       (which of 5 slots to write to)
  - When chunk_num advances: DEL the slot key (erasing data from 5 chunks ago),
    then start writing fresh. This naturally evicts the oldest chunk.

Redis key schema:
  md:hist:{exchange}:{market}:{symbol}:{slot}
  Type:    Redis List
  Element: "{ts_ms}:{bid}:{ask}"   (colon-separated string, compact)

Slot registry:
  md:hist:registry
  Type:  Redis Hash
  Field: "{slot}"  (string "0".."MAX_CHUNKS-1")
  Value: "{chunk_num}:{start_ts_ms}"

  Updated once per slot rotation (every CHUNK_SECONDS). Consumers read this
  hash to know which time window each slot covers and whether slot data is
  from the expected cycle or stale from a previous one.

Example:
  md:hist:binance:spot:BTCUSDT:2
  -> ["1710754899000:42500.50:42501.00",
      "1710754900123:42500.60:42501.10", ...]

  md:hist:registry
  -> {"0": "1451:1741234800000",
      "1": "1452:1741236000000",
      "2": "1453:1741237200000",
      "3": "1449:1741230000000",
      "4": "1450:1741231200000"}
"""
import asyncio
import os
import signal
import sys
import time

import orjson

sys.path.insert(0, os.path.dirname(__file__))
from common import get_redis, now_ms, setup_logging

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CHUNK_SECONDS: int = int(os.getenv("HIST_CHUNK_SECONDS", str(20 * 60)))  # 1200 s
MAX_CHUNKS: int    = int(os.getenv("HIST_MAX_CHUNKS", "5"))
BATCH_SIZE: int    = int(os.getenv("HIST_BATCH_SIZE", "200"))
BATCH_TIMEOUT: float = float(os.getenv("HIST_BATCH_TIMEOUT_MS", "100")) / 1000  # -> seconds

CHANNEL_PATTERN = "md:updates:*"
REGISTRY_KEY    = "md:hist:registry"


# ---------------------------------------------------------------------------
# Chunk helpers
# ---------------------------------------------------------------------------
def chunk_num_and_slot(ts_ms: int) -> tuple[int, int]:
    """Return (chunk_num, slot) for a given timestamp in milliseconds."""
    n = ts_ms // 1000 // CHUNK_SECONDS
    return n, n % MAX_CHUNKS


class RotationTracker:
    """
    Tracks the last known chunk_num per (exchange, market, symbol).
    Returns True when chunk_num advances, signalling that we must DEL
    the slot key before starting to write the new chunk.

    Also tracks which (slot, chunk_num) pairs have already been written
    to the registry hash so we only HSET once per rotation event.
    """

    def __init__(self) -> None:
        self._state: dict[tuple[str, str, str], int] = {}
        self._registered: set[tuple[int, int]] = set()  # (slot, chunk_num)

    def needs_rotation(self, exchange: str, market: str, symbol: str,
                       chunk_num: int) -> bool:
        key = (exchange, market, symbol)
        if self._state.get(key) != chunk_num:
            self._state[key] = chunk_num
            return True
        return False

    def needs_registry_update(self, slot: int, chunk_num: int) -> bool:
        """Return True the first time this (slot, chunk_num) pair is seen."""
        key = (slot, chunk_num)
        if key not in self._registered:
            self._registered.add(key)
            return True
        return False


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------
async def process_batch(
    redis_client,
    batch: list[tuple[str, str, str]],
    tracker: RotationTracker,
    log,
) -> None:
    """
    1. Pipeline-read bid / ask / ts_redis for every (exchange, market, symbol).
    2. Pipeline-write compact history records to the current chunk slot,
       deleting the slot key first when a new chunk starts.
    """
    if not batch:
        return

    chunk_num, slot = chunk_num_and_slot(now_ms())

    # --- Step 1: read current prices ---
    read_pipe = redis_client.pipeline()
    for exchange, market, symbol in batch:
        read_pipe.hmget(f"md:{exchange}:{market}:{symbol}", "bid", "ask", "ts_redis")
    results = await read_pipe.execute()

    # --- Step 2: write to history chunks ---
    write_pipe = redis_client.pipeline()
    wrote = 0

    for (exchange, market, symbol), fields in zip(batch, results):
        bid, ask, ts_raw = fields
        if bid is None or ask is None:
            continue  # key expired / not yet written

        ts = ts_raw if ts_raw else str(now_ms())
        hist_key = f"md:hist:{exchange}:{market}:{symbol}:{slot}"

        if tracker.needs_rotation(exchange, market, symbol, chunk_num):
            # Erase the slot that held data from MAX_CHUNKS ago
            write_pipe.delete(hist_key)
            log.debug("chunk_rotated",
                      source=f"{exchange}:{market}",
                      symbol=symbol,
                      new_chunk=chunk_num,
                      slot=slot)

            # Update registry once per (slot, chunk_num) — not per symbol
            if tracker.needs_registry_update(slot, chunk_num):
                start_ts_ms = chunk_num * CHUNK_SECONDS * 1000
                write_pipe.hset(REGISTRY_KEY, str(slot), f"{chunk_num}:{start_ts_ms}")
                log.info("registry_updated",
                         slot=slot, chunk_num=chunk_num, start_ts_ms=start_ts_ms)

        write_pipe.rpush(hist_key, f"{ts}:{bid}:{ask}")
        wrote += 1

    if wrote:
        await write_pipe.execute()


# ---------------------------------------------------------------------------
# Main subscription loop
# ---------------------------------------------------------------------------
async def run(shutdown_event: asyncio.Event, log) -> None:
    redis_client = get_redis()
    pubsub = redis_client.pubsub()
    tracker = RotationTracker()

    await pubsub.psubscribe(CHANNEL_PATTERN)
    log.info("subscribed", pattern=CHANNEL_PATTERN,
             chunk_seconds=CHUNK_SECONDS, max_chunks=MAX_CHUNKS)

    batch: list[tuple[str, str, str]] = []
    deadline = time.monotonic() + BATCH_TIMEOUT

    try:
        while not shutdown_event.is_set():
            # Non-blocking poll for one message (50 ms timeout)
            try:
                msg = await asyncio.wait_for(
                    pubsub.get_message(ignore_subscribe_messages=True),
                    timeout=0.05,
                )
            except asyncio.TimeoutError:
                msg = None

            if msg and msg.get("type") == "pmessage":
                # channel = "md:updates:{exchange}:{market}"
                channel: str = msg["channel"]
                parts = channel.split(":")
                if len(parts) >= 4:
                    exchange, market = parts[2], parts[3]
                    try:
                        data = orjson.loads(msg["data"])
                        symbol: str | None = data.get("symbol")
                        if symbol:
                            batch.append((exchange, market, symbol))
                    except Exception:
                        pass

            # Flush batch when it's full or the deadline has passed
            now = time.monotonic()
            if batch and (len(batch) >= BATCH_SIZE or now >= deadline):
                try:
                    await process_batch(redis_client, batch, tracker, log)
                except Exception as exc:
                    log.error("batch_error", error=str(exc), exc_info=True)
                batch = []
                deadline = time.monotonic() + BATCH_TIMEOUT

    finally:
        # Flush whatever is left before exiting
        if batch:
            try:
                await process_batch(redis_client, batch, tracker, log)
            except Exception:
                pass
        await pubsub.punsubscribe(CHANNEL_PATTERN)
        await pubsub.aclose()
        await redis_client.aclose()


async def main() -> None:
    log = setup_logging("price_history")
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    log.info("price_history_started",
             chunk_seconds=CHUNK_SECONDS, max_chunks=MAX_CHUNKS,
             batch_size=BATCH_SIZE, batch_timeout_ms=int(BATCH_TIMEOUT * 1000))
    try:
        await run(shutdown_event, log)
    finally:
        log.info("price_history_stopped")


if __name__ == "__main__":
    asyncio.run(main())
