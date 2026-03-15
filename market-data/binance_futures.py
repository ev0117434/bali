"""
Binance Futures (USD-M) WebSocket Collector
Identical to binance_spot.py except WS URL and ts_exchange = E field.
"""
import asyncio
import os
import random
import signal
import sys

import orjson
import websockets

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    RECONNECT_MAX_DELAY, REDIS_KEY_TTL, SYMBOLS_PER_CONN, WS_RECV_TIMEOUT,
    get_redis, load_symbols, now_ms, setup_logging,
)

EXCHANGE = "binance"
MARKET = "futures"
WS_URL = "wss://fstream.binance.com/ws"
PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL", "30"))
SYMBOLS_FILE = os.path.join(os.path.dirname(__file__), "..", "dictionaries", "subscribe", "binance", "binance_futures.txt")
REDIS_CHANNEL = f"md:updates:{EXCHANGE}:{MARKET}"

from collections import deque
_redis_buffer: deque = deque(maxlen=1000)


def parse_message(data: dict, log) -> dict | None:
    """Parse Binance Futures bookTicker. Returns None if message should be skipped."""
    if "result" in data:
        log.info("ws_subscribed", response=str(data.get("result")))
        return None
    if "s" not in data:
        return None
    # Futures bookTicker contains event time in field "E"
    return {
        "symbol": data["s"].upper(),
        "bid": data.get("b", ""),
        "bid_qty": data.get("B", ""),
        "ask": data.get("a", ""),
        "ask_qty": data.get("A", ""),
        "last": "",
        "ts_exchange": str(data.get("E", 0)),
    }


async def write_redis(redis_client, parsed: dict, ts_received: int, log) -> bool:
    global _redis_buffer
    ts_redis = now_ms()
    symbol = parsed["symbol"]
    key = f"md:{EXCHANGE}:{MARKET}:{symbol}"
    mapping = {
        "ts_exchange": parsed["ts_exchange"],
        "ts_received": str(ts_received),
        "ts_redis": str(ts_redis),
    }
    for field in ("bid", "bid_qty", "ask", "ask_qty", "last"):
        if parsed[field] != "":
            mapping[field] = parsed[field]
    payload = orjson.dumps({"symbol": symbol, "key": key}).decode()
    try:
        pipe = redis_client.pipeline()
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, REDIS_KEY_TTL)
        pipe.publish(REDIS_CHANNEL, payload)
        await pipe.execute()
        if _redis_buffer:
            flushed = 0
            while _redis_buffer:
                item = _redis_buffer.popleft()
                try:
                    p = redis_client.pipeline()
                    p.hset(item["key"], mapping=item["mapping"])
                    p.expire(item["key"], REDIS_KEY_TTL)
                    await p.execute()
                    flushed += 1
                except Exception:
                    _redis_buffer.appendleft(item)
                    break
            if flushed:
                log.info("redis_recovered", flushed=flushed)
        return True
    except Exception as exc:
        log.error("redis_error", error=str(exc), symbol=symbol, exc_info=True)
        if len(_redis_buffer) == _redis_buffer.maxlen:
            log.warning("redis_buffer_full", dropped_count=1)
        _redis_buffer.append({"key": key, "mapping": mapping})
        return False


async def ws_worker(symbols: list[str], conn_id: int, redis_client, shutdown_event: asyncio.Event, log):
    log = log.bind(conn_id=conn_id, symbols_count=len(symbols))
    attempt = 0
    params = [f"{s.lower()}@bookTicker" for s in symbols]

    while not shutdown_event.is_set():
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_INTERVAL * 2,
                open_timeout=30,
            ) as ws:
                log.info("ws_connected")
                sub_msg = orjson.dumps({"method": "SUBSCRIBE", "params": params, "id": conn_id}).decode()
                await ws.send(sub_msg)
                attempt = 0

                while not shutdown_event.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        log.warning("ws_recv_timeout", conn_id=conn_id)
                        break

                    ts_received = now_ms()
                    try:
                        data = orjson.loads(raw)
                    except orjson.JSONDecodeError:
                        log.warning("message_parse_error", raw=raw[:200])
                        continue

                    parsed = parse_message(data, log)
                    if parsed:
                        await write_redis(redis_client, parsed, ts_received, log)

                await ws.close(1000)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            delay = min(2 ** attempt, RECONNECT_MAX_DELAY) + random.uniform(0, 1)
            log.warning("ws_reconnecting", attempt=attempt, delay_sec=round(delay, 2), error=str(exc))
            await asyncio.sleep(delay)
            attempt += 1

    log.info("ws_worker_stopped")


async def main():
    log = setup_logging(f"{EXCHANGE}_{MARKET}")
    symbols = load_symbols(SYMBOLS_FILE)
    chunks = [symbols[i:i + SYMBOLS_PER_CONN] for i in range(0, len(symbols), SYMBOLS_PER_CONN)]
    redis_client = get_redis()
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    log.info("collector_started", symbols_count=len(symbols), connections=len(chunks))

    tasks = [
        asyncio.create_task(ws_worker(chunk, i, redis_client, shutdown_event, log))
        for i, chunk in enumerate(chunks)
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await redis_client.aclose()
        log.info("graceful_shutdown")


if __name__ == "__main__":
    asyncio.run(main())
