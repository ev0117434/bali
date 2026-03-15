#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dictionaries/main.py — Оркестратор обновления торговых пар.

Порядок выполнения:
  1. Binance REST API  — получение всех пар (spot + futures)
  2. Binance WebSocket — валидация активности пар (60 сек)
  3. Bybit REST API    — получение всех пар (spot + futures)
  4. Bybit WebSocket   — валидация активности пар (60 сек)
  5. Создание пересечений → combination/
  6. Создание файлов подписки → subscribe/
  7. Итоговый отчёт

Выходные файлы:
  combination/binance_spot_bybit_futures.txt  — binance_spot ∩ bybit_futures
  combination/bybit_spot_binance_futures.txt  — bybit_spot ∩ binance_futures

  subscribe/binance/binance_spot.txt          — уникальные пары из combination-файлов с 'binance_spot'
  subscribe/binance/binance_futures.txt       — уникальные пары из combination-файлов с 'binance_futures'
  subscribe/bybit/bybit_spot.txt              — уникальные пары из combination-файлов с 'bybit_spot'
  subscribe/bybit/bybit_futures.txt           — уникальные пары из combination-файлов с 'bybit_futures'
"""

import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

COMBINATION_DIR = BASE_DIR / "combination"
SUBSCRIBE_DIR = BASE_DIR / "subscribe"

BINANCE_SPOT_BYBIT_FUTURES_FILE = COMBINATION_DIR / "binance_spot_bybit_futures.txt"
BYBIT_SPOT_BINANCE_FUTURES_FILE = COMBINATION_DIR / "bybit_spot_binance_futures.txt"

SUBSCRIBE_BINANCE_SPOT_FILE    = SUBSCRIBE_DIR / "binance" / "binance_spot.txt"
SUBSCRIBE_BINANCE_FUTURES_FILE = SUBSCRIBE_DIR / "binance" / "binance_futures.txt"
SUBSCRIBE_BYBIT_SPOT_FILE      = SUBSCRIBE_DIR / "bybit" / "bybit_spot.txt"
SUBSCRIBE_BYBIT_FUTURES_FILE   = SUBSCRIBE_DIR / "bybit" / "bybit_futures.txt"


# ──────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции
# ──────────────────────────────────────────────────────────────────────────────

def _save(path: Path, pairs: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(pairs) + "\n", encoding="utf-8")


def _make_combinations(
    binance_spot: list,
    binance_futures: list,
    bybit_spot: list,
    bybit_futures: list,
) -> tuple:
    """
    Создаёт два файла пересечений:
      binance_spot ∩ bybit_futures  → combination/binance_spot_bybit_futures.txt
      bybit_spot   ∩ binance_futures → combination/bybit_spot_binance_futures.txt
    """
    binance_spot_set = set(binance_spot)
    binance_futures_set = set(binance_futures)
    bybit_spot_set = set(bybit_spot)
    bybit_futures_set = set(bybit_futures)

    bsbyf = sorted(binance_spot_set & bybit_futures_set)
    bysbf = sorted(bybit_spot_set & binance_futures_set)

    _save(BINANCE_SPOT_BYBIT_FUTURES_FILE, bsbyf)
    _save(BYBIT_SPOT_BINANCE_FUTURES_FILE, bysbf)

    return bsbyf, bysbf


def _make_subscribe_files() -> tuple:
    """
    Читает все файлы из combination/.
    Каждое ключевое слово в имени файла указывает, на какой subscribe-список пойдут пары:
      'binance_spot'    → subscribe/binance/binance_spot.txt
      'binance_futures' → subscribe/binance/binance_futures.txt
      'bybit_spot'      → subscribe/bybit/bybit_spot.txt
      'bybit_futures'   → subscribe/bybit/bybit_futures.txt
    Например, binance_spot_bybit_futures.txt → пары идут в binance_spot И bybit_futures.
    Возвращает (bn_spot, bn_futures, bb_spot, bb_futures).
    """
    bn_spot: set = set()
    bn_futures: set = set()
    bb_spot: set = set()
    bb_futures: set = set()

    for comb_file in COMBINATION_DIR.glob("*.txt"):
        name = comb_file.name
        lines = [
            l.strip()
            for l in comb_file.read_text(encoding="utf-8").splitlines()
            if l.strip()
        ]
        if "binance_spot" in name:
            bn_spot.update(lines)
        if "binance_futures" in name:
            bn_futures.update(lines)
        if "bybit_spot" in name:
            bb_spot.update(lines)
        if "bybit_futures" in name:
            bb_futures.update(lines)

    bs  = sorted(bn_spot)
    bf  = sorted(bn_futures)
    bys = sorted(bb_spot)
    byf = sorted(bb_futures)

    _save(SUBSCRIBE_BINANCE_SPOT_FILE,    bs)
    _save(SUBSCRIBE_BINANCE_FUTURES_FILE, bf)
    _save(SUBSCRIBE_BYBIT_SPOT_FILE,      bys)
    _save(SUBSCRIBE_BYBIT_FUTURES_FILE,   byf)

    return bs, bf, bys, byf


# ──────────────────────────────────────────────────────────────────────────────
# Отчёт
# ──────────────────────────────────────────────────────────────────────────────

def _print_report(r: dict, total_time: float) -> None:
    sep = "=" * 70
    print(f"\n{sep}")
    print("ИТОГОВЫЙ ОТЧЁТ")
    print(sep)

    print("\n[BINANCE] REST API:")
    print(f"   Spot пар:    {r['bn_spot_total']}")
    print(f"   Futures пар: {r['bn_fut_total']}")

    print("\n[BINANCE] WebSocket валидация:")
    pct_s = r['bn_spot_active'] / r['bn_spot_total'] * 100 if r['bn_spot_total'] else 0
    pct_f = r['bn_fut_active'] / r['bn_fut_total'] * 100 if r['bn_fut_total'] else 0
    print(f"   Spot active:    {r['bn_spot_active']:4d} / {r['bn_spot_total']} ({pct_s:.1f}%)")
    print(f"   Futures active: {r['bn_fut_active']:4d} / {r['bn_fut_total']} ({pct_f:.1f}%)")

    print("\n[BYBIT] REST API:")
    print(f"   Spot пар:    {r['bb_spot_total']}")
    print(f"   Futures пар: {r['bb_fut_total']}")

    print("\n[BYBIT] WebSocket валидация:")
    pct_s = r['bb_spot_active'] / r['bb_spot_total'] * 100 if r['bb_spot_total'] else 0
    pct_f = r['bb_fut_active'] / r['bb_fut_total'] * 100 if r['bb_fut_total'] else 0
    print(f"   Spot active:    {r['bb_spot_active']:4d} / {r['bb_spot_total']} ({pct_s:.1f}%)")
    print(f"   Futures active: {r['bb_fut_active']:4d} / {r['bb_fut_total']} ({pct_f:.1f}%)")

    print("\n[КОМБИНАЦИИ] combination/")
    print(f"   binance_spot ∩ bybit_futures   : {r['comb_bsbyf']:4d} пар -> {BINANCE_SPOT_BYBIT_FUTURES_FILE.name}")
    print(f"   bybit_spot   ∩ binance_futures : {r['comb_bysbf']:4d} пар -> {BYBIT_SPOT_BINANCE_FUTURES_FILE.name}")

    print("\n[ПОДПИСКА] subscribe/")
    print(f"   binance_spot.txt    : {r['sub_bn_spot']:4d} пар -> {SUBSCRIBE_BINANCE_SPOT_FILE}")
    print(f"   binance_futures.txt : {r['sub_bn_fut']:4d} пар -> {SUBSCRIBE_BINANCE_FUTURES_FILE}")
    print(f"   bybit_spot.txt      : {r['sub_bb_spot']:4d} пар -> {SUBSCRIBE_BYBIT_SPOT_FILE}")
    print(f"   bybit_futures.txt   : {r['sub_bb_fut']:4d} пар -> {SUBSCRIBE_BYBIT_FUTURES_FILE}")

    print(f"\nОбщее время: {total_time:.2f} сек")
    print(sep + "\n")


# ──────────────────────────────────────────────────────────────────────────────
# Главная функция
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    from binance.binance_pairs import fetch_pairs as binance_fetch_pairs
    from binance.binance_ws import validate_pairs as binance_validate_ws
    from bybit.bybit_pairs import fetch_pairs as bybit_fetch_pairs
    from bybit.bybit_ws import validate_pairs as bybit_validate_ws

    r = {}
    t0 = time.time()

    # ── 1. Binance REST API ───────────────────────────────────────────────────
    print("[1/6] Binance REST API: получение пар...", flush=True)
    binance_spot, binance_futures = binance_fetch_pairs()
    r['bn_spot_total'] = len(binance_spot)
    r['bn_fut_total'] = len(binance_futures)
    print(f"      Spot: {len(binance_spot)}, Futures: {len(binance_futures)}")

    # ── 2. Binance WebSocket ──────────────────────────────────────────────────
    print("[2/6] Binance WebSocket: валидация активных пар (60 сек)...", flush=True)
    t1 = time.time()
    active_bn_spot, active_bn_fut = binance_validate_ws(binance_spot, binance_futures)
    r['bn_spot_active'] = len(active_bn_spot)
    r['bn_fut_active'] = len(active_bn_fut)
    print(
        f"      Spot active: {len(active_bn_spot)}, "
        f"Futures active: {len(active_bn_fut)}  "
        f"({time.time() - t1:.0f}s)"
    )

    # ── 3. Bybit REST API ─────────────────────────────────────────────────────
    print("[3/6] Bybit REST API: получение пар...", flush=True)
    bybit_spot, bybit_futures = bybit_fetch_pairs()
    r['bb_spot_total'] = len(bybit_spot)
    r['bb_fut_total'] = len(bybit_futures)
    print(f"      Spot: {len(bybit_spot)}, Futures: {len(bybit_futures)}")

    # ── 4. Bybit WebSocket ────────────────────────────────────────────────────
    print("[4/6] Bybit WebSocket: валидация активных пар (60 сек)...", flush=True)
    t2 = time.time()
    active_bb_spot, active_bb_fut = bybit_validate_ws(bybit_spot, bybit_futures)
    r['bb_spot_active'] = len(active_bb_spot)
    r['bb_fut_active'] = len(active_bb_fut)
    print(
        f"      Spot active: {len(active_bb_spot)}, "
        f"Futures active: {len(active_bb_fut)}  "
        f"({time.time() - t2:.0f}s)"
    )

    # ── 5. Пересечения ────────────────────────────────────────────────────────
    print("[5/6] Создание пересечений (combination/)...", flush=True)
    bsbyf, bysbf = _make_combinations(
        active_bn_spot, active_bn_fut,
        active_bb_spot, active_bb_fut,
    )
    r['comb_bsbyf'] = len(bsbyf)
    r['comb_bysbf'] = len(bysbf)
    print(
        f"      binance_spot∩bybit_futures: {len(bsbyf)}, "
        f"bybit_spot∩binance_futures: {len(bysbf)}"
    )

    # ── 6. Файлы подписки ─────────────────────────────────────────────────────
    print("[6/6] Создание файлов подписки (subscribe/)...", flush=True)
    sub_bn_spot, sub_bn_fut, sub_bb_spot, sub_bb_fut = _make_subscribe_files()
    r['sub_bn_spot'] = len(sub_bn_spot)
    r['sub_bn_fut']  = len(sub_bn_fut)
    r['sub_bb_spot'] = len(sub_bb_spot)
    r['sub_bb_fut']  = len(sub_bb_fut)
    print(
        f"      binance_spot: {len(sub_bn_spot)}, binance_futures: {len(sub_bn_fut)}, "
        f"bybit_spot: {len(sub_bb_spot)}, bybit_futures: {len(sub_bb_fut)}"
    )

    # ── 7. Отчёт ──────────────────────────────────────────────────────────────
    _print_report(r, time.time() - t0)


if __name__ == "__main__":
    main()
