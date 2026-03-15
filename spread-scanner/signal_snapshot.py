"""
Signal Snapshot Logger

Слушает Redis-канал ch:spread_signals. При получении сигнала запускает
asyncio-задачу, которая в течение SNAPSHOT_DURATION_S секунд каждые
SNAPSHOT_INTERVAL_S секунд читает текущие цены из Redis и пишет строку в файл.

Файловая структура:
  signals/{dir_name}/{symbol}_{ts_signal}.csv

Формат каждой строки (включая первую — исходный сигнал):
  spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts

Переменные окружения:
  SNAPSHOT_INTERVAL_S   = 0.3    (интервал между строками, сек)
  SNAPSHOT_DURATION_S   = 3500   (длительность снапшота, сек)
  REDIS_URL             = redis://localhost:6379/0
"""
import asyncio
import os
import signal
import sys
import time

import orjson

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "market-data"))
from common import get_redis, now_ms, setup_logging

# ── Конфигурация ──────────────────────────────────────────────────────────────
SNAPSHOT_INTERVAL_S = float(os.getenv("SNAPSHOT_INTERVAL_S", "0.3"))
SNAPSHOT_DURATION_S = int(os.getenv("SNAPSHOT_DURATION_S",   "3500"))
SIGNAL_CHANNEL      = "ch:spread_signals"

_ROOT       = os.path.join(os.path.dirname(__file__), "..")
SIGNALS_DIR = os.path.join(_ROOT, "signals")

# Источники данных по направлению
_SOURCES = {
    "A": {"buy": ("binance", "spot"),  "sell": ("bybit",   "futures")},
    "B": {"buy": ("bybit",   "spot"),  "sell": ("binance", "futures")},
    "C": {"buy": ("okx",     "spot"),  "sell": ("binance", "futures")},
    "D": {"buy": ("binance", "spot"),  "sell": ("okx",     "futures")},
    "E": {"buy": ("okx",     "spot"),  "sell": ("bybit",   "futures")},
    "F": {"buy": ("bybit",   "spot"),  "sell": ("okx",     "futures")},
    "G": {"buy": ("gate",    "spot"),  "sell": ("binance", "futures")},
    "H": {"buy": ("binance", "spot"),  "sell": ("gate",    "futures")},
    "I": {"buy": ("gate",    "spot"),  "sell": ("bybit",   "futures")},
    "J": {"buy": ("bybit",   "spot"),  "sell": ("gate",    "futures")},
    "K": {"buy": ("gate",    "spot"),  "sell": ("okx",     "futures")},
    "L": {"buy": ("okx",     "spot"),  "sell": ("gate",    "futures")},
}

# Полные имена направлений для имён папок
DIRECTION_NAMES = {
    "A": "binance_s_bybit_f",
    "B": "bybit_s_binance_f",
    "C": "okx_s_binance_f",
    "D": "binance_s_okx_f",
    "E": "okx_s_bybit_f",
    "F": "bybit_s_okx_f",
    "G": "gate_s_binance_f",
    "H": "binance_s_gate_f",
    "I": "gate_s_bybit_f",
    "J": "bybit_s_gate_f",
    "K": "gate_s_okx_f",
    "L": "okx_s_gate_f",
}


def _csv_line(spot_exch: str, fut_exch: str, symbol: str,
              ask_spot, bid_futures, spread_pct, ts: int) -> str:
    """Формирует CSV-строку: spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts"""
    return f"{spot_exch},{fut_exch},{symbol},{ask_spot or ''},{bid_futures or ''},{spread_pct},{ts}"


# ── Снапшот-задача ─────────────────────────────────────────────────────────────

async def snapshot_task(redis, signal_data: dict, log) -> None:
    """
    Пишет снапшоты каждые SNAPSHOT_INTERVAL_S сек в течение SNAPSHOT_DURATION_S сек.
    Каждая строка — CSV: spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
    """
    direction = signal_data.get("direction", "")
    symbol    = signal_data.get("symbol",    "")
    ts_signal = signal_data.get("ts_signal", now_ms())

    src = _SOURCES.get(direction)
    if not src:
        log.warning("snapshot_unknown_direction", direction=direction, symbol=symbol)
        return

    bex, bmkt = src["buy"]
    sex, smkt = src["sell"]
    buy_key  = f"md:{bex}:{bmkt}:{symbol}"
    sell_key = f"md:{sex}:{smkt}:{symbol}"

    # signals/{dir_name}/{symbol}_{ts_signal}.csv
    dir_name = DIRECTION_NAMES.get(direction, direction)
    out_dir  = os.path.join(SIGNALS_DIR, dir_name)
    os.makedirs(out_dir, exist_ok=True)
    filepath = os.path.join(out_dir, f"{symbol}_{ts_signal}.csv")

    n        = 0
    deadline = time.monotonic() + SNAPSHOT_DURATION_S

    with open(filepath, "a", encoding="utf-8", buffering=1) as f:
        # Первая строка — исходный сигнал (те же цены, что зафиксировал scanner)
        first = _csv_line(
            bex, sex, symbol,
            signal_data.get("buy_ask"),
            signal_data.get("sell_bid"),
            signal_data.get("spread_pct", ""),
            ts_signal,
        )
        f.write(first + "\n")

        while time.monotonic() < deadline:
            tick = time.monotonic()

            try:
                pipe = redis.pipeline(transaction=False)
                pipe.hmget(buy_key,  "ask", "ask_qty", "ts_redis")
                pipe.hmget(sell_key, "bid", "bid_qty", "ts_redis")
                (buy_ask, _buy_qty, _buy_ts), (sell_bid, _sell_qty, _sell_ts) = await pipe.execute()
            except Exception as exc:
                log.error("snapshot_redis_error", symbol=symbol, direction=direction, error=str(exc))
                await asyncio.sleep(SNAPSHOT_INTERVAL_S)
                continue

            n += 1
            spread_pct = ""
            if buy_ask and sell_bid:
                try:
                    spread_pct = round(
                        (float(sell_bid) - float(buy_ask)) / float(buy_ask) * 100, 4
                    )
                except (ValueError, TypeError):
                    pass

            f.write(_csv_line(bex, sex, symbol, buy_ask, sell_bid, spread_pct, now_ms()) + "\n")

            sleep = SNAPSHOT_INTERVAL_S - (time.monotonic() - tick)
            if sleep > 0:
                await asyncio.sleep(sleep)

    log.info("snapshot_done", symbol=symbol, direction=dir_name, rows=n, file=filepath)


# ── Точка входа ───────────────────────────────────────────────────────────────

async def main() -> None:
    log   = setup_logging("signal_snapshot")
    redis = get_redis()

    pubsub = redis.pubsub()
    await pubsub.subscribe(SIGNAL_CHANNEL)

    shutdown = asyncio.Event()
    loop     = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)

    tasks: set[asyncio.Task] = set()

    log.info(
        "snapshot_logger_started",
        channel=SIGNAL_CHANNEL,
        duration_s=SNAPSHOT_DURATION_S,
        interval_s=SNAPSHOT_INTERVAL_S,
    )

    try:
        async for message in pubsub.listen():
            if shutdown.is_set():
                break
            if message["type"] != "message":
                continue
            try:
                sig_data = orjson.loads(message["data"])
            except Exception:
                continue

            direction = sig_data.get("direction", "")
            dir_name  = DIRECTION_NAMES.get(direction, direction)
            task = asyncio.create_task(
                snapshot_task(redis, sig_data, log),
                name=f"snap:{dir_name}:{sig_data.get('symbol')}",
            )
            tasks.add(task)
            task.add_done_callback(tasks.discard)

            log.info(
                "snapshot_started",
                symbol=sig_data.get("symbol"),
                direction=dir_name,
                active_tasks=len(tasks),
            )
    finally:
        if tasks:
            log.info("waiting_for_snapshots", count=len(tasks))
            await asyncio.gather(*tasks, return_exceptions=True)
        await pubsub.unsubscribe(SIGNAL_CHANNEL)
        await redis.aclose()
        log.info("graceful_shutdown")


if __name__ == "__main__":
    asyncio.run(main())
