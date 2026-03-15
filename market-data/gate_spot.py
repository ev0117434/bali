"""
Gate.io Spot WebSocket Collector
Subscribes to spot.book_ticker channel, writes best bid/ask to Redis.

Gate.io V4 protocol specifics:
- Subscribe: {"time": ts, "channel": "spot.book_ticker", "event": "subscribe", "payload": [...]}
  Payload is a list of native symbols (BTC_USDT), sent in batches of BATCH_SIZE
- Ping: {"time": ts, "channel": "spot.ping"}  (JSON, not plain text)
- Response: {"channel": "spot.book_ticker", "event": "update",
             "time": ts_sec, "result": {"s": "BTC_USDT", "b": bid, "B": bid_qty,
                                        "a": ask, "A": ask_qty}}
- Separate WS URLs for spot and futures
"""
import asyncio
import os
import random
import signal
import sys
import time

import orjson
import websockets

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    RECONNECT_MAX_DELAY, REDIS_KEY_TTL, SYMBOLS_PER_CONN, WS_RECV_TIMEOUT,
    get_redis, load_symbols, now_ms, setup_logging,
)

EXCHANGE = "gate"
MARKET = "spot"
WS_URL = "wss://api.gateio.ws/ws/v4/"
CHANNEL = "spot.book_ticker"
PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL", "20"))
BATCH_SIZE = 100  # Gate.io: max symbols per subscribe message
SYMBOLS_FILE = os.path.join(
    os.path.dirname(__file__), "..", "dictionaries", "subscribe", "gate", "gate_spot.txt"
)
REDIS_CHANNEL = f"md:updates:{EXCHANGE}:{MARKET}"

# Known quote currencies for reverse normalization BTCUSDT → BTC_USDT
_KNOWN_QUOTES = ["USDT", "USDC", "BTC", "ETH", "DAI"]

from collections import deque
_redis_buffer: deque = deque(maxlen=1000)


def _ts() -> int:
    return int(time.time())


def _to_native(symbol: str) -> str:
    """BTCUSDT → BTC_USDT (Gate.io native format)."""
    for q in _KNOWN_QUOTES:
        if symbol.endswith(q):
            return f"{symbol[:-len(q)]}_{q}"
    return symbol


def _normalize(sym: str) -> str:
    """BTC_USDT → BTCUSDT."""
    return sym.replace("_", "")


def parse_message(data: dict, log) -> dict | None:
    """
    Parse Gate.io V4 book_ticker update message.
    Returns None for non-data messages (subscribe ack, ping ack).
    """
    if not isinstance(data, dict):
        return None

    if data.get("channel") != CHANNEL:
        return None

    if data.get("event") != "update":
        return None

    result = data.get("result")
    if not result:
        return None

    sym = result.get("s", "")
    if not sym:
        return None

    # Gate.io book_ticker has no last price; outer "time" is epoch seconds
    ts_exchange = str(data.get("time", 0) * 1000)

    return {
        "symbol": _normalize(sym),
        "bid": result.get("b", ""),
        "bid_qty": result.get("B", ""),
        "ask": result.get("a", ""),
        "ask_qty": result.get("A", ""),
        "last": "",
        "ts_exchange": ts_exchange,
    }


async def write_redis(redis_client, parsed: dict, ts_received: int, log) -> bool:
    global _redis_buffer
    ts_redis = now_ms()
    symbol = parsed["symbol"]
    key = f"md:{EXCHANGE}:{MARKET}:{symbol}"
    mapping = {
        "bid": parsed["bid"],
        "bid_qty": parsed["bid_qty"],
        "ask": parsed["ask"],
        "ask_qty": parsed["ask_qty"],
        "last": parsed["last"],
        "ts_exchange": parsed["ts_exchange"],
        "ts_received": str(ts_received),
        "ts_redis": str(ts_redis),
    }
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


async def ping_loop(ws, shutdown_event: asyncio.Event, interval: int, log):
    """Send periodic JSON ping to Gate.io."""
    while not shutdown_event.is_set():
        await asyncio.sleep(interval)
        if shutdown_event.is_set():
            break
        try:
            await ws.send(orjson.dumps({"time": _ts(), "channel": "spot.ping"}).decode())
        except Exception:
            break


async def ws_worker(symbols: list[str], conn_id: int, redis_client, shutdown_event: asyncio.Event, log):
    log = log.bind(conn_id=conn_id, symbols_count=len(symbols))
    attempt = 0
    # Convert normalized symbols to Gate.io native format
    native = [_to_native(s) for s in symbols]

    while not shutdown_event.is_set():
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=None,  # Gate.io pings handled manually
                open_timeout=30,
            ) as ws:
                log.info("ws_connected")
                # Gate.io: send subscribe in batches of BATCH_SIZE
                for i in range(0, len(native), BATCH_SIZE):
                    batch = native[i:i + BATCH_SIZE]
                    sub_msg = orjson.dumps({
                        "time": _ts(),
                        "channel": CHANNEL,
                        "event": "subscribe",
                        "payload": batch,
                    }).decode()
                    await ws.send(sub_msg)
                    await asyncio.sleep(0.02)
                attempt = 0

                ping_task = asyncio.create_task(ping_loop(ws, shutdown_event, PING_INTERVAL, log))
                try:
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
                        if parsed and parsed.get("symbol"):
                            await write_redis(redis_client, parsed, ts_received, log)
                finally:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)

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
