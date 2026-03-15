"""
Latency Monitor
Periodically samples Redis keys and reports latency statistics.
Uses a rolling SCAN cursor so all keys are covered over time.
"""
import asyncio
import os
import signal
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from common import get_redis, now_ms, setup_logging

SAMPLING_INTERVAL_SEC: int = int(os.getenv("SAMPLING_INTERVAL_SEC", "10"))
SAMPLING_MAX_KEYS: int = int(os.getenv("SAMPLING_MAX_KEYS", "200"))
ANOMALY_WARN_MS: int = int(os.getenv("ANOMALY_WARN_MS", "1000"))
ANOMALY_CRIT_MS: int = int(os.getenv("ANOMALY_CRIT_MS", "5000"))


def calc_stats(values: list[float]) -> dict:
    """Calculate min, max, avg, p95 for a list of numbers."""
    if not values:
        return {}
    s = sorted(values)
    p95_idx = max(0, int(len(s) * 0.95) - 1)
    return {
        "min": round(s[0], 2),
        "max": round(s[-1], 2),
        "avg": round(sum(s) / len(s), 2),
        "p95": round(s[p95_idx], 2),
    }


def parse_latencies(ts_exchange: str, ts_received: str, ts_redis: str) -> dict | None:
    """
    Compute latency deltas from three timestamps.
    Returns None if any value is missing or unparseable.
    Binance Spot: ts_exchange == ts_received (no exchange ts) — skip e2e.
    """
    try:
        ts_e = int(ts_exchange)
        ts_r = int(ts_received)
        ts_rd = int(ts_redis)
    except (ValueError, TypeError):
        return None

    collector_to_redis = ts_rd - ts_r

    # Binance Spot workaround: ts_exchange is artificially set to ts_received
    # so exchange→collector would be 0 and e2e would equal collector_to_redis
    # Skip these to avoid misleading stats
    if ts_e == ts_r or ts_e == 0:
        return {
            "exchange_to_collector": None,
            "collector_to_redis": collector_to_redis,
            "end_to_end": None,
            "clock_skew": False,
        }

    exchange_to_collector = ts_r - ts_e
    end_to_end = ts_rd - ts_e
    clock_skew = exchange_to_collector < 0

    return {
        "exchange_to_collector": exchange_to_collector,
        "collector_to_redis": collector_to_redis,
        "end_to_end": end_to_end,
        "clock_skew": clock_skew,
    }


async def run(redis_client, shutdown_event: asyncio.Event, log):
    scan_cursor = 0
    # Accumulate samples per market between report cycles
    e2e_by_market: dict[str, list[float]] = defaultdict(list)
    c2r_by_market: dict[str, list[float]] = defaultdict(list)
    anomaly_count: dict[str, int] = defaultdict(int)

    while not shutdown_event.is_set():
        # One SCAN step with rolling cursor
        scan_cursor, keys = await redis_client.scan(
            scan_cursor, match="md:*:*:*", count=SAMPLING_MAX_KEYS
        )

        for key in keys:
            vals = await redis_client.hmget(key, "ts_exchange", "ts_received", "ts_redis")
            ts_exchange, ts_received, ts_redis_val = vals
            if not all([ts_exchange, ts_received, ts_redis_val]):
                continue

            lats = parse_latencies(ts_exchange, ts_received, ts_redis_val)
            if lats is None:
                continue

            # Parse market key: md:{exchange}:{market}:{symbol}
            parts = key.split(":")
            if len(parts) < 4:
                continue
            market_key = f"{parts[1]}:{parts[2]}"

            if lats["clock_skew"]:
                log.info("clock_skew_detected", key=key, skew_ms=lats["exchange_to_collector"])

            c2r_by_market[market_key].append(lats["collector_to_redis"])

            symbol = parts[3] if len(parts) > 3 else ""
            if symbol == "BTCUSDT" and lats["end_to_end"] is not None:
                e2e_val = lats["end_to_end"]
                e2e_by_market[market_key].append(e2e_val)

                if e2e_val > ANOMALY_CRIT_MS:
                    log.critical("latency_anomaly", key=key, e2e_ms=e2e_val)
                    anomaly_count[market_key] += 1
                elif e2e_val > ANOMALY_WARN_MS:
                    log.warning("latency_anomaly", key=key, e2e_ms=e2e_val)
                    anomaly_count[market_key] += 1

        # Emit report when cursor wraps (full cycle) or on each interval
        if scan_cursor == 0 and (e2e_by_market or c2r_by_market):
            all_markets = set(list(e2e_by_market.keys()) + list(c2r_by_market.keys()))
            for market_key in all_markets:
                exchange, market = market_key.split(":", 1)
                e2e_samples = e2e_by_market.get(market_key, [])
                c2r_samples = c2r_by_market.get(market_key, [])
                log.info(
                    "latency_report",
                    exchange=exchange,
                    market=market,
                    samples=len(c2r_samples),
                    e2e_ms=calc_stats(e2e_samples),
                    collector_to_redis_ms=calc_stats(c2r_samples),
                    anomalies=anomaly_count.get(market_key, 0),
                )
            e2e_by_market.clear()
            c2r_by_market.clear()
            anomaly_count.clear()

        try:
            await asyncio.wait_for(
                asyncio.shield(shutdown_event.wait()),
                timeout=SAMPLING_INTERVAL_SEC,
            )
        except asyncio.TimeoutError:
            pass


async def main():
    log = setup_logging("latency_monitor")
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
