"""
Stale Monitor
Scans Redis every SCAN_INTERVAL_SEC seconds.
Symbols not updated for more than STALE_THRESHOLD_SEC are reported as stale.
"""
import asyncio
import os
import signal
import sys

import orjson

sys.path.insert(0, os.path.dirname(__file__))
from common import get_redis, now_ms, setup_logging

STALE_THRESHOLD_SEC: int = int(os.getenv("STALE_THRESHOLD_SEC", "60"))
SCAN_INTERVAL_SEC: int = int(os.getenv("SCAN_INTERVAL_SEC", "30"))
SCAN_COUNT: int = int(os.getenv("SCAN_COUNT", "500"))
GRACE_CYCLES: int = 2  # cycles to wait before emitting alerts


async def scan_once(redis_client) -> tuple[list[dict], int]:
    """
    Full SCAN of md:*:*:* keys.
    Returns (stale_list, total_keys_scanned).
    """
    stale: list[dict] = []
    total = 0
    cursor = 0
    threshold_ms = STALE_THRESHOLD_SEC * 1000

    while True:
        cursor, keys = await redis_client.scan(cursor, match="md:*:*:*", count=SCAN_COUNT)
        total += len(keys)

        for key in keys:
            ts_redis = await redis_client.hget(key, "ts_redis")
            if ts_redis is None:
                # Key exists but has no ts_redis — corrupted entry
                continue
            try:
                age_ms = now_ms() - int(ts_redis)
            except (ValueError, TypeError):
                continue

            if age_ms > threshold_ms:
                stale.append({"key": key, "age_sec": age_ms // 1000})

        if cursor == 0:
            break

    return stale, total


async def run(redis_client, shutdown_event: asyncio.Event, log):
    cycle = 0
    previously_stale: set[str] = set()

    while not shutdown_event.is_set():
        cycle += 1
        in_grace = cycle <= GRACE_CYCLES

        try:
            stale, total = await scan_once(redis_client)
        except Exception as exc:
            log.error("scan_error", error=str(exc), exc_info=True)
            await asyncio.sleep(SCAN_INTERVAL_SEC)
            continue

        current_stale_keys = {s["key"] for s in stale}

        # Recovery: was stale, now fresh
        recovered = previously_stale - current_stale_keys
        for key in recovered:
            log.info("stale_recovered", key=key)

        if not in_grace:
            for s in stale:
                log.warning("stale_detected", key=s["key"], age_sec=s["age_sec"])

            if stale:
                alert = orjson.dumps({
                    "ts": now_ms(),
                    "stale_count": len(stale),
                    "symbols": stale,
                }).decode()
                await redis_client.publish("alerts:stale", alert)

        log.info(
            "stale_scan_complete",
            cycle=cycle,
            total_keys=total,
            stale_count=len(stale),
            in_grace=in_grace,
        )

        previously_stale = current_stale_keys

        try:
            await asyncio.wait_for(
                asyncio.shield(shutdown_event.wait()),
                timeout=SCAN_INTERVAL_SEC,
            )
        except asyncio.TimeoutError:
            pass  # Normal: time to scan again


async def main():
    log = setup_logging("stale_monitor")
    redis_client = get_redis()
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    log.info("collector_started")

    try:
        await run(redis_client, shutdown_event, log)
    finally:
        await redis_client.aclose()
        log.info("graceful_shutdown")


if __name__ == "__main__":
    asyncio.run(main())
