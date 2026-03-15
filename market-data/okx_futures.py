"""
OKX Futures (Perpetual SWAP) WebSocket Collector
Subscribes to tickers channel for all symbols, writes best bid/ask to Redis.

OKX V5 protocol specifics:
- Subscribe: {"op": "subscribe", "args": [{"channel": "tickers", "instId": "BTC-USDT-SWAP"}]}
- Ping: plain text "ping", pong response: plain text "pong"
- Up to 300 instId per connection
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

EXCHANGE = "okx"
MARKET = "futures"
WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL", "25"))
OKX_CHUNK_SIZE = 300  # OKX allows up to 300 instId per connection
SYMBOLS_FILE = os.path.join(
    os.path.dirname(__file__), "..", "dictionaries", "subscribe", "okx", "okx_futures.txt"
)
REDIS_CHANNEL = f"md:updates:{EXCHANGE}:{MARKET}"

# Known OKX quote currencies (ordered by frequency) for reverse normalization
_KNOWN_QUOTES = ["USDT", "USDC", "BTC", "ETH", "DAI", "OKB", "USDK"]

from collections import deque
_redis_buffer: deque = deque(maxlen=1000)


def _to_native(symbol: str) -> str:
    """BTCUSDT → BTC-USDT-SWAP (OKX perpetual futures instId format)."""
    for q in _KNOWN_QUOTES:
        if symbol.endswith(q):
            return f"{symbol[:-len(q)]}-{q}-SWAP"
    return f"{symbol}-SWAP"


def _normalize(inst_id: str) -> str:
    """BTC-USDT-SWAP → BTCUSDT."""
    s = inst_id
    if s.endswith("-SWAP"):
        s = s[:-5]
    return s.replace("-", "")


def parse_message(data: dict, log) -> dict | None:
    """
    Parse OKX V5 tickers channel message for futures (SWAP).
    Returns None for control messages (subscribe/error confirmations).
    """
    if not isinstance(data, dict):
        return None

    # Subscribe/error confirmation events
    if data.get("event") in ("subscribe", "error", "unsubscribe"):
        if data.get("event") == "error":
            log.warning("ws_subscribe_error", msg=data.get("msg", ""), code=data.get("code", ""))
        return None

    arg = data.get("arg", {})
    if arg.get("channel") != "tickers":
        return None

    data_list = data.get("data")
    if not data_list:
        return None

    d = data_list[0]
    inst_id = d.get("instId", "")
    if not inst_id:
        return None

    # Only process SWAP instruments
    if not inst_id.endswith("-SWAP"):
        return None

    return {
        "symbol": _normalize(inst_id),
        "bid": d.get("bidPx", ""),
        "bid_qty": d.get("bidSz", ""),
        "ask": d.get("askPx", ""),
        "ask_qty": d.get("askSz", ""),
        "last": d.get("last", ""),
        "ts_exchange": d.get("ts", "0"),
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


async def ping_loop(ws, shutdown_event: asyncio.Event, interval: int, log):
    """Send periodic ping to OKX to keep connection alive."""
    while not shutdown_event.is_set():
        await asyncio.sleep(interval)
        if shutdown_event.is_set():
            break
        try:
            await ws.send("ping")
        except Exception:
            break


async def ws_worker(symbols: list[str], conn_id: int, redis_client, shutdown_event: asyncio.Event, log):
    log = log.bind(conn_id=conn_id, symbols_count=len(symbols))
    attempt = 0
    # Convert normalized symbols to OKX native futures instId format
    args = [{"channel": "tickers", "instId": _to_native(s)} for s in symbols]

    while not shutdown_event.is_set():
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=None,  # OKX pings handled manually
                open_timeout=30,
            ) as ws:
                log.info("ws_connected")
                sub_msg = orjson.dumps({"op": "subscribe", "args": args}).decode()
                await ws.send(sub_msg)
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

                        # OKX pong is a plain text string
                        if raw == "pong":
                            continue

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
    chunk_size = min(SYMBOLS_PER_CONN, OKX_CHUNK_SIZE)
    chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
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
