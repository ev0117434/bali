#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Простой рабочий скрипт:
- Читает пары (по одной в строке) из:
  /root/siro/dictionaries/temp/all_pairs/binance/binance_spot_all_temp.txt
  /root/siro/dictionaries/temp/all_pairs/binance/binance_futures_usdm_all_temp.txt
- Подключается к WS Binance (Spot и Futures USD-M) на bookTicker (best bid/ask)
- В течение заданного времени (в коде) отмечает пары, по которым пришёл хотя бы 1 ответ
- Сохраняет новые списки:
  /root/siro/dictionaries/all_pairs/binance/binance_spot.txt
  /root/siro/dictionaries/all_pairs/binance/binance_futures.txt
- В конце печатает подробные логи в консоль
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException


# ====== НАСТРОЙКИ ======
DURATION_SECONDS = 60        # <-- окно проверки (секунды). Меняйте здесь.
CHUNK_SIZE = 300             # чанки по 300 стримов на соединение

SPOT_INPUT = "/root/siro/dictionaries/temp/all_pairs/binance/binance_spot_all_temp.txt"
FUTURES_INPUT = "/root/siro/dictionaries/temp/all_pairs/binance/binance_futures_usdm_all_temp.txt"

OUT_DIR = "/root/siro/dictionaries/all_pairs/binance"
SPOT_OUTPUT = os.path.join(OUT_DIR, "binance_spot.txt")
FUTURES_OUTPUT = os.path.join(OUT_DIR, "binance_futures.txt")

SPOT_BASE = "wss://stream.binance.com:9443/stream?streams="
FUTURES_BASE = "wss://fstream.binance.com/stream?streams="  # USD-M futures combined streams
# =======================


def read_pairs(path: str) -> List[str]:
    pairs: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            pairs.append(s.upper())
    # Уберём дубли с сохранением порядка
    seen = set()
    uniq = []
    for p in pairs:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


def chunk_list(items: List[str], n: int) -> List[List[str]]:
    return [items[i:i + n] for i in range(0, len(items), n)]


def build_combined_url(base: str, symbols: List[str]) -> str:
    # stream name: symbol lower + "@bookTicker"
    streams = "/".join([f"{s.lower()}@bookTicker" for s in symbols])
    return base + streams


@dataclass
class ConnStats:
    idx: int
    symbols: List[str]
    url: str
    started_at: float = 0.0
    ended_at: float = 0.0
    messages: int = 0
    unique_symbols_received: int = 0
    errors: List[str] = field(default_factory=list)
    closed_normally: bool = False


async def ws_consumer(
    name: str,
    url: str,
    received: Set[str],
    stats: ConnStats,
    stop_at: float,
) -> None:
    stats.started_at = time.time()
    try:
        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_queue=4096,
        ) as ws:
            while True:
                now = time.time()
                if now >= stop_at:
                    stats.closed_normally = True
                    break

                # Ожидаем сообщение, но не дольше чем до конца окна
                timeout = max(0.05, stop_at - now)
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    continue

                stats.messages += 1
                try:
                    obj = json.loads(msg)
                    data = obj.get("data", obj)
                    sym = data.get("s")
                    if sym:
                        sym = sym.upper()
                        if sym not in received:
                            received.add(sym)
                            stats.unique_symbols_received += 1
                except Exception as e:
                    stats.errors.append(f"json_parse_error: {type(e).__name__}: {e}")

    except (ConnectionClosed, WebSocketException, OSError) as e:
        stats.errors.append(f"ws_error: {type(e).__name__}: {e}")
    except Exception as e:
        stats.errors.append(f"unexpected_error: {type(e).__name__}: {e}")
    finally:
        stats.ended_at = time.time()


async def run_market(market_name: str, base_url: str, symbols: List[str]) -> Tuple[Set[str], List[ConnStats]]:
    chunks = chunk_list(symbols, CHUNK_SIZE)

    received: Set[str] = set()
    stats_list: List[ConnStats] = []

    stop_at = time.time() + DURATION_SECONDS

    tasks = []
    for i, chunk in enumerate(chunks, start=1):
        url = build_combined_url(base_url, chunk)
        st = ConnStats(idx=i, symbols=chunk, url=url)
        stats_list.append(st)
        tasks.append(asyncio.create_task(ws_consumer(market_name, url, received, st, stop_at)))

    # Ждём окончания окна (и закрытия задач)
    await asyncio.gather(*tasks, return_exceptions=True)
    return received, stats_list


def write_list(path: str, items: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for x in items:
            f.write(x + "\n")


def print_report(market: str, total_symbols: List[str], received: Set[str], stats_list: List[ConnStats], started: float, ended: float) -> None:
    total = len(total_symbols)
    ok = [s for s in total_symbols if s in received]
    bad = [s for s in total_symbols if s not in received]

    dur = ended - started
    total_msgs = sum(s.messages for s in stats_list)
    total_new = sum(s.unique_symbols_received for s in stats_list)
    total_errs = sum(len(s.errors) for s in stats_list)

    print("\n" + "=" * 90)
    print(f"[{market}] ОТЧЁТ")
    print("-" * 90)
    print(f"Время окна: {DURATION_SECONDS} сек")
    print(f"Фактическая длительность выполнения: {dur:.2f} сек")
    print(f"Всего пар на входе: {total}")
    print(f"Ответили (>=1 bookTicker): {len(ok)}")
    print(f"Не ответили: {len(bad)}")
    print(f"Чанков/соединений: {len(stats_list)} (по {CHUNK_SIZE} стримов на соединение)")
    print(f"Всего сообщений WS: {total_msgs}")
    print(f"Новых уникальных пар, впервые замеченных: {total_new}")
    print(f"Всего ошибок (суммарно по соединениям): {total_errs}")

    print("\nДетально по соединениям:")
    for st in stats_list:
        conn_dur = (st.ended_at - st.started_at) if (st.started_at and st.ended_at) else 0.0
        print("-" * 90)
        print(f"  Conn #{st.idx}: streams={len(st.symbols)} | msgs={st.messages} | new_syms={st.unique_symbols_received} | "
              f"dur={conn_dur:.2f}s | closed_normally={st.closed_normally}")
        if st.errors:
            print(f"    Errors ({len(st.errors)}):")
            # печатаем все ошибки, но компактно
            for e in st.errors:
                print(f"      - {e}")

    if bad:
        print("\nНе ответили (покажу первые 50):")
        for s in bad[:50]:
            print(f"  {s}")
        if len(bad) > 50:
            print(f"  ... и ещё {len(bad) - 50} шт.")

    print("=" * 90 + "\n")


async def main() -> None:
    t0 = time.time()

    spot_symbols = read_pairs(SPOT_INPUT)
    fut_symbols = read_pairs(FUTURES_INPUT)

    print(f"Старт. Окно проверки = {DURATION_SECONDS} сек, chunk_size = {CHUNK_SIZE}")
    print(f"Spot input:    {SPOT_INPUT}  (пар: {len(spot_symbols)})")
    print(f"Futures input: {FUTURES_INPUT}  (пар: {len(fut_symbols)})")
    print("Подключаюсь к WS...")

    spot_start = time.time()
    spot_received, spot_stats = await run_market("SPOT", SPOT_BASE, spot_symbols)
    spot_end = time.time()

    fut_start = time.time()
    fut_received, fut_stats = await run_market("FUTURES_USDM", FUTURES_BASE, fut_symbols)
    fut_end = time.time()

    # Итоговые списки в исходном порядке
    spot_ok = [s for s in spot_symbols if s in spot_received]
    fut_ok = [s for s in fut_symbols if s in fut_received]

    # Запись файлов
    write_list(SPOT_OUTPUT, spot_ok)
    write_list(FUTURES_OUTPUT, fut_ok)

    t1 = time.time()

    # Отчёты
    print_report("SPOT", spot_symbols, spot_received, spot_stats, spot_start, spot_end)
    print_report("FUTURES_USDM", fut_symbols, fut_received, fut_stats, fut_start, fut_end)

    print("Файлы сохранены:")
    print(f"  SPOT   -> {SPOT_OUTPUT}   (пар: {len(spot_ok)})")
    print(f"  FUTURE -> {FUTURES_OUTPUT}   (пар: {len(fut_ok)})")
    print(f"Готово. Общее время: {t1 - t0:.2f} сек")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановлено пользователем (Ctrl+C).")
