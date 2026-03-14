#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OKX WS (V5 Public) — проверка, по каким парам пришёл хотя бы 1 ответ с best bid/ask.

- Читает пары (по одной в строке) из 2 файлов (spot и futures/perp/derivatives список instId).
- Подключается к OKX Public WS: wss://ws.okx.com:8443/ws/v5/public
- Подписывается на канал "tickers" (в нём есть best bid/ask: bidPx/askPx).
- В течение WINDOW_SECONDS отмечает пары, по которым пришло >=1 сообщение.
- Сохраняет новые списки: okx_spot.txt и okx_futures.txt
- В конце печатает максимально подробные логи.

ВАЖНО про формат символов:
- OKX использует instId вида: BTC-USDT (spot), BTC-USDT-SWAP (perp), BTC-USDT-240628 (futures) и т.д.
- Если во входном файле строки без дефиса (например BTCUSDT) — скрипт попробует конвертировать в BTC-USDT
  по известным котировкам. Для деривативов суффиксы (-SWAP / -YYYYMMDD) он НЕ угадает, поэтому
  для OKX futures лучше давать уже готовые instId.
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException


# ================== НАСТРОЙКИ ==================
WINDOW_SECONDS = 60      # окно проверки (сек) — зашито
CHUNK_SIZE = 300           # чанки по 300 instId на соединение (как просили)
CONNECT_TIMEOUT = 10
PING_INTERVAL = 20

# !!! ПОДСТАВЬТЕ ВАШИ РЕАЛЬНЫЕ ПУТИ ДЛЯ OKX !!!
SPOT_INPUT = "/root/siro/dictionaries/temp/all_pairs/okx/okx_spot_all_temp.txt"
FUTURES_INPUT = "/root/siro/dictionaries/temp/all_pairs/okx/okx_futures_usdm_all_temp.txt"

OUT_DIR = "/root/siro/dictionaries/all_pairs/okx"
SPOT_OUTPUT = os.path.join(OUT_DIR, "okx_spot.txt")
FUTURES_OUTPUT = os.path.join(OUT_DIR, "okx_futures.txt")

OKX_PUBLIC_WS = "wss://ws.okx.com:8443/ws/v5/public"

# OKX subscribe requests should not be spammed. Мы делаем 1 subscribe на соединение (<=300 args),
# и небольшой sleep между соединениями.
CONNECT_STAGGER_SEC = 0.15
# ===============================================


# Для конвертации BTCUSDT -> BTC-USDT (если в файлах без дефисов)
KNOWN_QUOTES = [
    "USDT", "USDC", "USD",
    "BTC", "ETH",
    "EUR", "TRY", "BRL", "RUB", "UAH",
    "GBP", "AUD", "IDR", "VND", "NGN",
    "JPY", "KRW", "MXN", "ARS",
]


def read_pairs(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    out = []
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if s:
            out.append(s)
    # uniq keep order
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def write_pairs(path: str, pairs: List[str]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text("\n".join(pairs) + ("\n" if pairs else ""), encoding="utf-8")


def chunk_list(items: List[str], n: int) -> List[List[str]]:
    return [items[i:i + n] for i in range(0, len(items), n)]


def to_okx_instid(raw: str) -> Tuple[str, bool]:
    """
    Возвращает (instId, converted_flag)
    - Если уже содержит '-' => считаем готовым instId.
    - Иначе пытаемся BTCUSDT -> BTC-USDT по известным quote.
    """
    s = raw.strip().upper()
    if "-" in s:
        return s, False

    for q in KNOWN_QUOTES:
        if s.endswith(q) and len(s) > len(q):
            base = s[:-len(q)]
            return f"{base}-{q}", True

    # не смогли — вернём как есть
    return s, False


@dataclass
class ConnStats:
    idx: int
    ws_url: str
    instids: List[str]
    started_at: float = 0.0
    ended_at: float = 0.0
    msgs: int = 0
    tickers_msgs: int = 0
    other_msgs: int = 0
    sub_sent: bool = False
    sub_ack_ok: int = 0
    sub_ack_err: int = 0
    first_msg_ts: Optional[float] = None
    errors: List[str] = field(default_factory=list)
    closed_normally: bool = False


async def okx_consumer(
    ws_url: str,
    instids: List[str],                 # то, что реально подписываем
    instid_to_raw: Dict[str, str],      # чтобы в responded писать исходные строки
    responded_raw: Set[str],
    stats: ConnStats,
    stop_at: float,
) -> None:
    stats.started_at = time.time()
    sub_msg = {
        "op": "subscribe",
        "args": [{"channel": "tickers", "instId": iid} for iid in instids]
    }

    try:
        async with websockets.connect(
            ws_url,
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_INTERVAL,
            close_timeout=3,
            open_timeout=CONNECT_TIMEOUT,
            max_queue=4096,
        ) as ws:
            await ws.send(json.dumps(sub_msg))
            stats.sub_sent = True

            while True:
                now = time.time()
                if now >= stop_at:
                    stats.closed_normally = True
                    break

                timeout = max(0.05, stop_at - now)
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    continue

                stats.msgs += 1
                if stats.first_msg_ts is None:
                    stats.first_msg_ts = time.time()

                try:
                    msg = json.loads(raw)
                except Exception as e:
                    stats.other_msgs += 1
                    stats.errors.append(f"json_parse_error: {type(e).__name__}: {e}")
                    continue

                # ack/error events
                # примеры:
                # {"event":"subscribe","arg":{"channel":"tickers","instId":"BTC-USDT"}}
                # {"event":"error","msg":"...","code":"...","arg":{...}}
                ev = msg.get("event")
                if ev == "subscribe":
                    stats.sub_ack_ok += 1
                    continue
                if ev == "error":
                    stats.sub_ack_err += 1
                    stats.errors.append(f"subscribe_error_event: {msg}")
                    continue

                # data push:
                # {"arg":{"channel":"tickers","instId":"BTC-USDT"}, "data":[{"bidPx":"...","askPx":"..."}]}
                arg = msg.get("arg")
                if isinstance(arg, dict) and arg.get("channel") == "tickers":
                    stats.tickers_msgs += 1
                    iid = arg.get("instId")
                    if iid:
                        iid = str(iid).upper()
                        raw_sym = instid_to_raw.get(iid)
                        if raw_sym and raw_sym not in responded_raw:
                            responded_raw.add(raw_sym)
                    continue

                stats.other_msgs += 1

    except (ConnectionClosed, WebSocketException, OSError) as e:
        stats.errors.append(f"ws_error: {type(e).__name__}: {e}")
    except Exception as e:
        stats.errors.append(f"unexpected_error: {type(e).__name__}: {e}")
    finally:
        stats.ended_at = time.time()


async def run_market(market_name: str, raw_pairs: List[str]) -> Tuple[Set[str], List[ConnStats], Dict[str, int]]:
    # raw -> instId mapping (с сохранением исходных строк)
    instid_to_raw: Dict[str, str] = {}
    converted_cnt = 0
    unchanged_cnt = 0
    unknown_cnt = 0

    instids: List[str] = []
    for raw in raw_pairs:
        iid, conv = to_okx_instid(raw)
        if iid == raw.strip().upper() and "-" not in iid and not conv:
            # не получилось сконвертировать и не было дефиса
            unknown_cnt += 1
        elif conv:
            converted_cnt += 1
        else:
            unchanged_cnt += 1

        # если дубли instId — оставим первое raw
        iid_u = iid.upper()
        if iid_u not in instid_to_raw:
            instid_to_raw[iid_u] = raw.strip().upper()
            instids.append(iid_u)

    chunks = chunk_list(instids, CHUNK_SIZE)
    responded_raw: Set[str] = set()
    stats_list: List[ConnStats] = []
    stop_at = time.time() + WINDOW_SECONDS

    tasks = []
    for i, chunk in enumerate(chunks, start=1):
        st = ConnStats(idx=i, ws_url=OKX_PUBLIC_WS, instids=chunk)
        stats_list.append(st)
        tasks.append(asyncio.create_task(okx_consumer(OKX_PUBLIC_WS, chunk, instid_to_raw, responded_raw, st, stop_at)))
        await asyncio.sleep(CONNECT_STAGGER_SEC)

    await asyncio.gather(*tasks, return_exceptions=True)

    conv_stats = {
        "converted": converted_cnt,
        "unchanged": unchanged_cnt,
        "unknown_format": unknown_cnt,
        "instids_unique": len(instids),
        "raw_unique": len(raw_pairs),
        "connections": len(chunks),
    }
    return responded_raw, stats_list, conv_stats


def print_report(market: str, raw_pairs: List[str], responded_raw: Set[str], stats_list: List[ConnStats], conv_stats: Dict[str, int]) -> None:
    total = len(raw_pairs)
    ok = [p.strip().upper() for p in raw_pairs if p.strip().upper() in responded_raw]
    bad_cnt = total - len(ok)

    total_msgs = sum(s.msgs for s in stats_list)
    tick_msgs = sum(s.tickers_msgs for s in stats_list)
    other_msgs = sum(s.other_msgs for s in stats_list)
    sub_ok = sum(s.sub_ack_ok for s in stats_list)
    sub_err = sum(s.sub_ack_err for s in stats_list)
    total_errs = sum(len(s.errors) for s in stats_list)

    print("\n" + "=" * 90)
    print(f"[OKX {market}] ОТЧЁТ")
    print("-" * 90)
    print(f"WINDOW_SECONDS: {WINDOW_SECONDS}")
    print(f"raw_pairs_total: {total}")
    print(f"responded_pairs: {len(ok)}")
    print(f"not_responded: {bad_cnt}")
    print(f"chunk_size: {CHUNK_SIZE}")
    print(f"connections: {conv_stats['connections']}")
    print(f"unique_instIds: {conv_stats['instids_unique']}")
    print(f"format_converted(raw->instId): {conv_stats['converted']}")
    print(f"format_unchanged: {conv_stats['unchanged']}")
    print(f"format_unknown(no '-' and cannot split): {conv_stats['unknown_format']}")
    print(f"ws_msgs_total: {total_msgs}")
    print(f"tickers_msgs: {tick_msgs}")
    print(f"other_msgs: {other_msgs}")
    print(f"subscribe_ack_ok: {sub_ok}")
    print(f"subscribe_ack_err: {sub_err}")
    print(f"errors_total: {total_errs}")

    print("\nДетально по соединениям:")
    for st in stats_list:
        dur = (st.ended_at - st.started_at) if (st.started_at and st.ended_at) else 0.0
        print("-" * 90)
        print(
            f"  Conn #{st.idx}: instIds={len(st.instids)} | msgs={st.msgs} | tickers={st.tickers_msgs} | "
            f"other={st.other_msgs} | sub_sent={st.sub_sent} | sub_ok={st.sub_ack_ok} | sub_err={st.sub_ack_err} | "
            f"dur={dur:.2f}s | first_msg_ts={st.first_msg_ts} | closed_normally={st.closed_normally}"
        )
        if st.errors:
            print(f"    Errors ({len(st.errors)}):")
            for e in st.errors:
                print(f"      - {e}")

    print("=" * 90 + "\n")


async def main() -> None:
    spot_raw = read_pairs(SPOT_INPUT)
    fut_raw = read_pairs(FUTURES_INPUT)

    print("=== OKX CHECK START ===")
    print(f"WINDOW_SECONDS={WINDOW_SECONDS} CHUNK_SIZE={CHUNK_SIZE}")
    print(f"SPOT_INPUT:    {SPOT_INPUT} ({len(spot_raw)})")
    print(f"FUTURES_INPUT: {FUTURES_INPUT} ({len(fut_raw)})")
    print(f"WS: {OKX_PUBLIC_WS}")
    print("Подключаюсь...")

    spot_start = time.time()
    spot_ok, spot_stats, spot_conv = await run_market("SPOT", spot_raw)
    spot_end = time.time()

    fut_start = time.time()
    fut_ok, fut_stats, fut_conv = await run_market("FUTURES", fut_raw)
    fut_end = time.time()

    # сохраняем в исходном порядке и в исходном формате строк
    spot_out = [p.strip().upper() for p in spot_raw if p.strip().upper() in spot_ok]
    fut_out = [p.strip().upper() for p in fut_raw if p.strip().upper() in fut_ok]

    write_pairs(SPOT_OUTPUT, spot_out)
    write_pairs(FUTURES_OUTPUT, fut_out)

    print_report("SPOT", spot_raw, spot_ok, spot_stats, spot_conv)
    print_report("FUTURES", fut_raw, fut_ok, fut_stats, fut_conv)

    print("Файлы сохранены:")
    print(f"  SPOT   -> {SPOT_OUTPUT}   (пар: {len(spot_out)})")
    print(f"  FUTURE -> {FUTURES_OUTPUT}   (пар: {len(fut_out)})")
    print(f"Готово. Общее время: {time.time() - min(spot_start, fut_start):.2f} сек")
    print("=== OKX CHECK DONE ===")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановлено пользователем (Ctrl+C).")
