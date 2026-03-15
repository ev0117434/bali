#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dictionaries/main.py — Оркестратор обновления торговых пар.

Порядок выполнения:
  1. Binance REST API  — получение всех пар (spot + futures)
  2. Binance WebSocket — валидация активности пар (60 сек)
  3. Bybit REST API    — получение всех пар (spot + futures)
  4. Bybit WebSocket   — валидация активности пар (60 сек)
  5. OKX REST API     — получение всех пар (spot + futures/swap)
  6. OKX WebSocket    — валидация активности пар (60 сек)
  7. Создание пересечений → combination/
  8. Создание файлов подписки → subscribe/
  9. Итоговый отчёт

Комбинации (combination/):
  binance_spot_bybit_futures.txt   — binance_spot ∩ bybit_futures
  bybit_spot_binance_futures.txt   — bybit_spot   ∩ binance_futures
  binance_spot_okx_futures.txt     — binance_spot ∩ okx_futures
  okx_spot_binance_futures.txt     — okx_spot     ∩ binance_futures
  bybit_spot_okx_futures.txt       — bybit_spot   ∩ okx_futures
  okx_spot_bybit_futures.txt       — okx_spot     ∩ bybit_futures

Файлы подписки (subscribe/):
  Логика: каждое ключевое слово в имени combination-файла определяет
  subscribe-файл. Например, binance_spot_okx_futures.txt →
    subscribe/binance/binance_spot.txt  +  subscribe/okx/okx_futures.txt
"""

import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

COMBINATION_DIR = BASE_DIR / "combination"
SUBSCRIBE_DIR   = BASE_DIR / "subscribe"

# ── Combination files ──────────────────────────────────────────────────────────
COMBINATIONS = [
    ("binance_spot", "bybit_futures",   "binance_spot_bybit_futures.txt"),
    ("bybit_spot",   "binance_futures", "bybit_spot_binance_futures.txt"),
    ("binance_spot", "okx_futures",     "binance_spot_okx_futures.txt"),
    ("okx_spot",     "binance_futures", "okx_spot_binance_futures.txt"),
    ("bybit_spot",   "okx_futures",     "bybit_spot_okx_futures.txt"),
    ("okx_spot",     "bybit_futures",   "okx_spot_bybit_futures.txt"),
]

# ── Subscribe files ────────────────────────────────────────────────────────────
# Все возможные ключевые слова и куда они маппятся
SUBSCRIBE_MAP = {
    "binance_spot":    SUBSCRIBE_DIR / "binance" / "binance_spot.txt",
    "binance_futures": SUBSCRIBE_DIR / "binance" / "binance_futures.txt",
    "bybit_spot":      SUBSCRIBE_DIR / "bybit"   / "bybit_spot.txt",
    "bybit_futures":   SUBSCRIBE_DIR / "bybit"   / "bybit_futures.txt",
    "okx_spot":        SUBSCRIBE_DIR / "okx"     / "okx_spot.txt",
    "okx_futures":     SUBSCRIBE_DIR / "okx"     / "okx_futures.txt",
}


# ──────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции
# ──────────────────────────────────────────────────────────────────────────────

def _save(path: Path, pairs: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(pairs) + "\n", encoding="utf-8")


def _make_combinations(active: dict) -> dict:
    """
    Создаёт все combination-файлы как пересечения активных пар.
    active = {"binance_spot": [...], "binance_futures": [...], ...}
    Возвращает словарь {filename: count}.
    """
    COMBINATION_DIR.mkdir(parents=True, exist_ok=True)
    results = {}
    for key_a, key_b, filename in COMBINATIONS:
        set_a = set(active.get(key_a, []))
        set_b = set(active.get(key_b, []))
        intersection = sorted(set_a & set_b)
        _save(COMBINATION_DIR / filename, intersection)
        results[filename] = len(intersection)
    return results


def _make_subscribe_files() -> dict:
    """
    Читает все *.txt из combination/.
    Для каждого ключевого слова из SUBSCRIBE_MAP, найденного в имени файла,
    добавляет пары в соответствующий subscribe-файл (без дублей).
    Возвращает {keyword: count}.
    """
    buckets: dict = {key: set() for key in SUBSCRIBE_MAP}

    for comb_file in COMBINATION_DIR.glob("*.txt"):
        name = comb_file.name
        lines = [
            l.strip()
            for l in comb_file.read_text(encoding="utf-8").splitlines()
            if l.strip()
        ]
        for key in SUBSCRIBE_MAP:
            if key in name:
                buckets[key].update(lines)

    counts = {}
    for key, path in SUBSCRIBE_MAP.items():
        pairs = sorted(buckets[key])
        _save(path, pairs)
        counts[key] = len(pairs)
    return counts


# ──────────────────────────────────────────────────────────────────────────────
# Отчёт
# ──────────────────────────────────────────────────────────────────────────────

def _print_report(r: dict, comb_counts: dict, sub_counts: dict, total_time: float) -> None:
    sep = "=" * 70
    print(f"\n{sep}")
    print("ИТОГОВЫЙ ОТЧЁТ")
    print(sep)

    for exch, label in [("bn", "BINANCE"), ("bb", "BYBIT"), ("okx", "OKX")]:
        st = r[f"{exch}_spot_total"]
        ft = r[f"{exch}_fut_total"]
        sa = r[f"{exch}_spot_active"]
        fa = r[f"{exch}_fut_active"]
        pct_s = sa / st * 100 if st else 0
        pct_f = fa / ft * 100 if ft else 0
        print(f"\n[{label}] REST API:  Spot={st}, Futures={ft}")
        print(f"[{label}] WS active: Spot={sa}/{st} ({pct_s:.1f}%), Futures={fa}/{ft} ({pct_f:.1f}%)")

    print(f"\n[КОМБИНАЦИИ] combination/")
    for _, _, fname in COMBINATIONS:
        cnt = comb_counts.get(fname, 0)
        print(f"   {cnt:4d} пар  {fname}")

    print(f"\n[ПОДПИСКА] subscribe/")
    for key, path in SUBSCRIBE_MAP.items():
        cnt = sub_counts.get(key, 0)
        print(f"   {cnt:4d} пар  {path.relative_to(BASE_DIR)}")

    print(f"\nОбщее время: {total_time:.2f} сек")
    print(sep + "\n")


# ──────────────────────────────────────────────────────────────────────────────
# Главная функция
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    from binance.binance_pairs import fetch_pairs as binance_fetch
    from binance.binance_ws    import validate_pairs as binance_ws
    from bybit.bybit_pairs     import fetch_pairs as bybit_fetch
    from bybit.bybit_ws        import validate_pairs as bybit_ws
    from okx.okx_pairs         import fetch_pairs as okx_fetch, load_native as okx_load_native
    from okx.okx_ws            import validate_pairs as okx_ws

    r = {}
    t0 = time.time()

    # ── 1. Binance REST ───────────────────────────────────────────────────────
    print("[1/8] Binance REST API: получение пар...", flush=True)
    bn_spot, bn_fut = binance_fetch()
    r["bn_spot_total"] = len(bn_spot)
    r["bn_fut_total"]  = len(bn_fut)
    print(f"      Spot: {len(bn_spot)}, Futures: {len(bn_fut)}")

    # ── 2. Binance WS ─────────────────────────────────────────────────────────
    print("[2/8] Binance WebSocket: валидация (60 сек)...", flush=True)
    t1 = time.time()
    abn_spot, abn_fut = binance_ws(bn_spot, bn_fut)
    r["bn_spot_active"] = len(abn_spot)
    r["bn_fut_active"]  = len(abn_fut)
    print(f"      Spot active: {len(abn_spot)}, Futures active: {len(abn_fut)}  ({time.time()-t1:.0f}s)")

    # ── 3. Bybit REST ─────────────────────────────────────────────────────────
    print("[3/8] Bybit REST API: получение пар...", flush=True)
    bb_spot, bb_fut = bybit_fetch()
    r["bb_spot_total"] = len(bb_spot)
    r["bb_fut_total"]  = len(bb_fut)
    print(f"      Spot: {len(bb_spot)}, Futures: {len(bb_fut)}")

    # ── 4. Bybit WS ───────────────────────────────────────────────────────────
    print("[4/8] Bybit WebSocket: валидация (60 сек)...", flush=True)
    t2 = time.time()
    abb_spot, abb_fut = bybit_ws(bb_spot, bb_fut)
    r["bb_spot_active"] = len(abb_spot)
    r["bb_fut_active"]  = len(abb_fut)
    print(f"      Spot active: {len(abb_spot)}, Futures active: {len(abb_fut)}  ({time.time()-t2:.0f}s)")

    # ── 5. OKX REST ───────────────────────────────────────────────────────────
    print("[5/8] OKX REST API: получение пар...", flush=True)
    okx_spot_norm, okx_fut_norm = okx_fetch()
    r["okx_spot_total"] = len(okx_spot_norm)
    r["okx_fut_total"]  = len(okx_fut_norm)
    print(f"      Spot: {len(okx_spot_norm)}, Futures: {len(okx_fut_norm)}")

    # ── 6. OKX WS ─────────────────────────────────────────────────────────────
    print("[6/8] OKX WebSocket: валидация (60 сек)...", flush=True)
    t3 = time.time()
    okx_spot_native, okx_fut_native = okx_load_native()
    aokx_spot, aokx_fut = okx_ws(okx_spot_native, okx_fut_native, okx_spot_norm, okx_fut_norm)
    r["okx_spot_active"] = len(aokx_spot)
    r["okx_fut_active"]  = len(aokx_fut)
    print(f"      Spot active: {len(aokx_spot)}, Futures active: {len(aokx_fut)}  ({time.time()-t3:.0f}s)")

    # ── 7. Пересечения ────────────────────────────────────────────────────────
    print("[7/8] Создание пересечений (combination/)...", flush=True)
    active = {
        "binance_spot":    abn_spot,
        "binance_futures": abn_fut,
        "bybit_spot":      abb_spot,
        "bybit_futures":   abb_fut,
        "okx_spot":        aokx_spot,
        "okx_futures":     aokx_fut,
    }
    comb_counts = _make_combinations(active)
    for fname, cnt in comb_counts.items():
        print(f"      {cnt:4d} пар  {fname}")

    # ── 8. Файлы подписки ─────────────────────────────────────────────────────
    print("[8/8] Создание файлов подписки (subscribe/)...", flush=True)
    sub_counts = _make_subscribe_files()
    for key, cnt in sub_counts.items():
        print(f"      {cnt:4d} пар  {key}")

    # ── 9. Отчёт ──────────────────────────────────────────────────────────────
    _print_report(r, comb_counts, sub_counts, time.time() - t0)


if __name__ == "__main__":
    main()
