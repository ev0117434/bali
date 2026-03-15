"""
Bybit Spot WebSocket Collector
Subscribes to tickers stream for all symbols, writes best bid/ask to Redis.
"""
import asyncio
import os
import random
import signal
import sys
from collections import deque

import orjson
import websockets

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    RECONNECT_MAX_DELAY, REDIS_KEY_TTL, SYMBOLS_PER_CONN, WS_RECV_TIMEOUT,
    get_redis, load_symbols, now_ms, setup_logging,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
EXCHANGE = "bybit"
MARKET = "spot"
WS_URL = "wss://stream.bybit.com/v5/public/spot"
PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL", "20"))
STATS_INTERVAL = int(os.getenv("WS_STATS_INTERVAL", "60"))
SYMBOLS_FILE = os.path.join(
    os.path.dirname(__file__), "..", "dictionaries", "subscribe", "bybit", "bybit_spot.txt"
)
REDIS_CHANNEL = f"md:updates:{EXCHANGE}:{MARKET}"
REDIS_KEY_PREFIX = f"md:{EXCHANGE}:{MARKET}"


# ---------------------------------------------------------------------------
# Message parsing
# ---------------------------------------------------------------------------
def parse_ticker(data: dict) -> dict | None:
    """
    Parse Bybit V5 tickers message.
    Returns None for control messages (op=subscribe/ping/pong) and
    non-ticker topics. Returns a flat dict on data messages.
    """
    if data.get("op") in ("subscribe", "ping", "pong"):
        return None

    if not data.get("topic", "").startswith("tickers."):
        return None

    d = data.get("data")
    if not d:
        return None

    return {
        "symbol":      d.get("symbol", "").upper(),
        "bid":         d.get("bid1Price", ""),
        "bid_qty":     d.get("bid1Size", ""),
        "ask":         d.get("ask1Price", ""),
        "ask_qty":     d.get("ask1Size", ""),
        "last":        d.get("lastPrice", ""),
        "ts_exchange": str(data.get("ts", 0)),
    }


# ---------------------------------------------------------------------------
# Redis writer — per-worker, with local write-back buffer
# ---------------------------------------------------------------------------
class RedisWriter:
    def __init__(self, redis_client, log, buffer_size: int = 1000):
        self._r = redis_client
        self._log = log
        self._buf: deque = deque(maxlen=buffer_size)

    async def write(self, parsed: dict, ts_received: int) -> None:
        symbol = parsed["symbol"]
        key = f"{REDIS_KEY_PREFIX}:{symbol}"
        mapping: dict = {
            "ts_exchange": parsed["ts_exchange"],
            "ts_received": str(ts_received),
            "ts_redis":    str(now_ms()),
        }
        for field in ("bid", "bid_qty", "ask", "ask_qty", "last"):
            if parsed[field] != "":
                mapping[field] = parsed[field]
        payload = orjson.dumps({"symbol": symbol, "key": key}).decode()

        try:
            pipe = self._r.pipeline()
            pipe.hset(key, mapping=mapping)
            pipe.expire(key, REDIS_KEY_TTL)
            pipe.publish(REDIS_CHANNEL, payload)
            await pipe.execute()
            await self._flush_buffer()
        except Exception as exc:
            self._log.error("redis_write_error", symbol=symbol, error=str(exc))
            if len(self._buf) == self._buf.maxlen:
                self._log.warning("redis_buffer_full", dropped=symbol)
            self._buf.append({"key": key, "mapping": mapping})

    async def _flush_buffer(self) -> None:
        if not self._buf:
            return
        flushed = 0
        while self._buf:
            item = self._buf.popleft()
            try:
                pipe = self._r.pipeline()
                pipe.hset(item["key"], mapping=item["mapping"])
                pipe.expire(item["key"], REDIS_KEY_TTL)
                await pipe.execute()
                flushed += 1
            except Exception:
                self._buf.appendleft(item)
                break
        if flushed:
            self._log.info("redis_buffer_flushed", flushed=flushed, remaining=len(self._buf))


# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------
async def _ping_loop(ws, shutdown_event: asyncio.Event, interval: int, log) -> None:
    """Send periodic pings to Bybit to keep the connection alive."""
    try:
        while not shutdown_event.is_set():
            await asyncio.sleep(interval)
            if shutdown_event.is_set():
                break
            await ws.send(orjson.dumps({"op": "ping"}).decode())
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        log.warning("ping_loop_error", error=str(exc), error_type=type(exc).__name__)


async def _stats_loop(counter: list, shutdown_event: asyncio.Event, interval: int, log) -> None:
    """Log message throughput every `interval` seconds."""
    try:
        while not shutdown_event.is_set():
            await asyncio.sleep(interval)
            if shutdown_event.is_set():
                break
            msgs, counter[0] = counter[0], 0
            log.info("stats", msgs_per_interval=msgs, interval_sec=interval)
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# Receive loop (extracted for clarity)
# ---------------------------------------------------------------------------
async def _read_loop(
    ws,
    writer: RedisWriter,
    shutdown_event: asyncio.Event,
    counter: list,
    log,
) -> None:
    """
    Read messages until: shutdown, recv timeout, or connection error.
    Caller is responsible for reconnecting after this returns.
    """
    while not shutdown_event.is_set():
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT)
        except asyncio.TimeoutError:
            log.warning("ws_recv_timeout", timeout_sec=WS_RECV_TIMEOUT)
            return

        ts_received = now_ms()

        try:
            data = orjson.loads(raw)
        except Exception:
            log.warning("json_parse_error", raw=raw[:200])
            continue

        op = data.get("op")

        if op == "subscribe":
            if data.get("success"):
                log.info("subscribed_ok", args=data.get("args", []))
            else:
                log.error(
                    "subscribe_failed",
                    ret_msg=data.get("ret_msg", ""),
                    ret_code=data.get("ret_code"),
                    args=data.get("args", []),
                )
            continue

        if op == "ping":
            await ws.send(orjson.dumps({"op": "pong"}).decode())
            continue

        if op == "pong":
            continue

        parsed = parse_ticker(data)
        if parsed and parsed["symbol"]:
            counter[0] += 1
            await writer.write(parsed, ts_received)


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------
async def ws_worker(
    symbols: list[str],
    conn_id: int,
    redis_client,
    shutdown_event: asyncio.Event,
    log,
) -> None:
    log = log.bind(conn_id=conn_id, symbols_count=len(symbols))
    writer = RedisWriter(redis_client, log)
    sub_args = [f"tickers.{s}" for s in symbols]
    sub_msg = orjson.dumps({"op": "subscribe", "args": sub_args}).decode()
    counter = [0]
    attempt = 0

    while not shutdown_event.is_set():
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=None,  # Bybit manages pings manually
                open_timeout=30,
                close_timeout=5,
            ) as ws:
                log.info("ws_connected", attempt=attempt)
                await ws.send(sub_msg)
                attempt = 0

                bg = [
                    asyncio.create_task(_ping_loop(ws, shutdown_event, PING_INTERVAL, log)),
                    asyncio.create_task(_stats_loop(counter, shutdown_event, STATS_INTERVAL, log)),
                ]
                try:
                    await _read_loop(ws, writer, shutdown_event, counter, log)
                finally:
                    for t in bg:
                        t.cancel()
                    await asyncio.gather(*bg, return_exceptions=True)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            delay = min(2 ** attempt, RECONNECT_MAX_DELAY) + random.uniform(0, 1)
            log.warning(
                "ws_disconnected",
                attempt=attempt,
                reconnect_in=round(delay, 1),
                error=str(exc),
                error_type=type(exc).__name__,
            )
            await asyncio.sleep(delay)
            attempt += 1

    log.info("ws_worker_stopped")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main() -> None:
    log = setup_logging(f"{EXCHANGE}_{MARKET}")
    symbols = load_symbols(SYMBOLS_FILE)
    chunks = [symbols[i : i + SYMBOLS_PER_CONN] for i in range(0, len(symbols), SYMBOLS_PER_CONN)]
    redis_client = get_redis()
    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    log.info(
        "collector_started",
        symbols_total=len(symbols),
        connections=len(chunks),
        ws_url=WS_URL,
    )

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
        log.info("shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())
