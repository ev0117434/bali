#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import os
import signal
import time
from pathlib import Path
from typing import List, Set, Optional, Tuple

import websockets

# ====== CONFIG ======
SPOT_WS_URL = "wss://stream.bybit.com/v5/public/spot"
FUTURES_WS_URL = "wss://stream.bybit.com/v5/public/linear"

SPOT_IN_PATH = Path("/root/siro/dictionaries/temp/all_pairs/bybit/bybit_spot_all_temp.txt")
FUTURES_IN_PATH = Path("/root/siro/dictionaries/temp/all_pairs/bybit/bybit_futures_usdm_all_temp.txt")

SPOT_OUT_PATH = Path("/root/siro/dictionaries/all_pairs/bybit/bybit_spot.txt")
FUTURES_OUT_PATH = Path("/root/siro/dictionaries/all_pairs/bybit/bybit_futures.txt")

DURATION_SECONDS = 60

# Spot: безопасно держать по 10 в одном subscribe (как и для tickers)
SPOT_SUB_BATCH = 10

# Linear: можно больше, но держим разумно
FUTURES_SUB_BATCH = 200

PING_INTERVAL_SEC = 20
SUBSCRIBE_SEND_DELAY_SEC = 0.02

# Ограничение на длину args в одном запросе, чтобы не улететь в слишком большой frame
MAX_ARGS_CHARS_PER_REQUEST = 20000
# =====================


def read_symbols(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    syms: List[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                syms.append(s)

    # de-dup keep order
    seen = set()
    out: List[str] = []
    for s in syms:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_symbols(path: Path, symbols: Set[str]) -> None:
    ensure_parent(path)
    data = "\n".join(sorted(symbols)) + ("\n" if symbols else "")
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.replace(tmp, path)


def chunk_by_limits(symbols: List[str], max_items: int, topic_prefix: str) -> List[List[str]]:
    batches: List[List[str]] = []
    cur: List[str] = []
    cur_chars = 0

    for sym in symbols:
        topic = f"{topic_prefix}.{sym}"  # e.g. orderbook.1.BTCUSDT
        add_chars = len(topic) + 3

        if cur and (len(cur) >= max_items or (cur_chars + add_chars) > MAX_ARGS_CHARS_PER_REQUEST):
            batches.append(cur)
            cur = []
            cur_chars = 0

        cur.append(sym)
        cur_chars += add_chars

    if cur:
        batches.append(cur)
    return batches


def parse_orderbook_l1_message(msg: dict) -> Optional[Tuple[str, Optional[str], Optional[str]]]:
    """
    For orderbook.1.{symbol} (snapshot only):
    data example fields include:
      - s: symbol
      - b: [[bidPrice, bidSize], ...] (for depth 1 -> one level)
      - a: [[askPrice, askSize], ...]
    We return (symbol, best_bid_price, best_ask_price) when detected.
    """
    topic = msg.get("topic") or ""
    # topic like "orderbook.1.BTCUSDT"
    if not topic.startswith("orderbook.1."):
        return None

    data = msg.get("data")
    if not isinstance(data, dict):
        return None

    sym = data.get("s")
    if not sym:
        # fallback: parse from topic
        sym = topic.split(".")[-1] if "." in topic else None
    if not sym:
        return None

    best_bid = None
    best_ask = None

    b = data.get("b")
    if isinstance(b, list) and b:
        first = b[0]
        if isinstance(first, list) and len(first) >= 1:
            best_bid = first[0]

    a = data.get("a")
    if isinstance(a, list) and a:
        first = a[0]
        if isinstance(first, list) and len(first) >= 1:
            best_ask = first[0]

    return sym, best_bid, best_ask


async def send_subscriptions(ws, symbols: List[str], batch_size: int, topic_prefix: str) -> None:
    batches = chunk_by_limits(symbols, max_items=batch_size, topic_prefix=topic_prefix)
    for b in batches:
        args = [f"{topic_prefix}.{s}" for s in b]
        payload = {"op": "subscribe", "args": args}
        await ws.send(json.dumps(payload))
        if SUBSCRIBE_SEND_DELAY_SEC:
            await asyncio.sleep(SUBSCRIBE_SEND_DELAY_SEC)


async def ping_loop(ws, stop_evt: asyncio.Event) -> None:
    try:
        while not stop_evt.is_set():
            await asyncio.sleep(PING_INTERVAL_SEC)
            if stop_evt.is_set():
                break
            await ws.send(json.dumps({"op": "ping"}))
    except asyncio.CancelledError:
        return
    except Exception:
        return


async def ws_collect_responded(
    url: str,
    symbols: List[str],
    batch_size: int,
    duration_sec: int,
    topic_prefix: str,
    name: str,
) -> Set[str]:
    responded: Set[str] = set()
    stop_evt = asyncio.Event()

    async def receiver(ws):
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue

                # Optional: print subscribe errors (one-liner, не спамит)
                # Bybit often returns {"success":false,"ret_msg":"...","op":"subscribe"} on errors
                if msg.get("op") == "subscribe" and msg.get("success") is False:
                    print(f"[{name}] SUBSCRIBE ERROR: {msg.get('ret_msg')}")

                ob = parse_orderbook_l1_message(msg)
                if ob is None:
                    continue

                sym, best_bid, best_ask = ob
                # "хотя бы одного сообщения" => считаем ответом сразу
                responded.add(sym)

        except asyncio.CancelledError:
            return
        except Exception:
            return

    async with websockets.connect(
        url,
        ping_interval=None,  # custom ping {"op":"ping"}
        max_queue=2048,
        close_timeout=3,
    ) as ws:
        await send_subscriptions(ws, symbols, batch_size=batch_size, topic_prefix=topic_prefix)

        recv_task = asyncio.create_task(receiver(ws))
        ping_task = asyncio.create_task(ping_loop(ws, stop_evt))

        try:
            await asyncio.sleep(duration_sec)
        finally:
            stop_evt.set()
            recv_task.cancel()
            ping_task.cancel()
            try:
                await ws.close()
            except Exception:
                pass
            await asyncio.gather(recv_task, ping_task, return_exceptions=True)

    return responded


async def main() -> int:
    spot_syms = read_symbols(SPOT_IN_PATH)
    fut_syms = read_symbols(FUTURES_IN_PATH)

    stop_main = asyncio.Event()

    def _handle_sig(*_):
        stop_main.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_sig)
        except Exception:
            pass

    t0 = time.time()

    # For fastest best bid/ask use orderbook.1 (snapshot only, very fast)
    topic_prefix = "orderbook.1"

    spot_task = asyncio.create_task(
        ws_collect_responded(SPOT_WS_URL, spot_syms, SPOT_SUB_BATCH, DURATION_SECONDS, topic_prefix, "SPOT")
    )
    fut_task = asyncio.create_task(
        ws_collect_responded(FUTURES_WS_URL, fut_syms, FUTURES_SUB_BATCH, DURATION_SECONDS, topic_prefix, "FUT")
    )
    stop_task = asyncio.create_task(stop_main.wait())

    done, pending = await asyncio.wait(
        {spot_task, fut_task, stop_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    if stop_task in done and stop_main.is_set():
        for t in (spot_task, fut_task):
            t.cancel()
        await asyncio.gather(spot_task, fut_task, return_exceptions=True)
        return 130

    spot_ok, fut_ok = await asyncio.gather(spot_task, fut_task)

    if not stop_task.done():
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)

    write_symbols(SPOT_OUT_PATH, spot_ok)
    write_symbols(FUTURES_OUT_PATH, fut_ok)

    elapsed = time.time() - t0
    print(f"Done in {elapsed:.2f}s")
    print(f"Spot responded: {len(spot_ok)}/{len(spot_syms)} -> {SPOT_OUT_PATH}")
    print(f"Futures responded: {len(fut_ok)}/{len(fut_syms)} -> {FUTURES_OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
