#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path

BASE_DIR = Path("/root/siro/dictionaries")
ALL_PAIRS_DIR = BASE_DIR / "all_pairs"
OUT_DIR = BASE_DIR / "combination"

# ВХОДНЫЕ ФАЙЛЫ (8 списков)
FILES = {
    ("binance", "spot"):    ALL_PAIRS_DIR / "binance" / "binance_spot.txt",
    ("binance", "futures"): ALL_PAIRS_DIR / "binance" / "binance_futures.txt",
    ("bybit", "spot"):      ALL_PAIRS_DIR / "bybit"   / "bybit_spot.txt",
    ("bybit", "futures"):   ALL_PAIRS_DIR / "bybit"   / "bybit_futures.txt",
    ("mexc", "spot"):       ALL_PAIRS_DIR / "mexc"    / "mexc_spot.txt",
    ("mexc", "futures"):    ALL_PAIRS_DIR / "mexc"    / "mexc_futures.txt",
    ("okx", "spot"):        ALL_PAIRS_DIR / "okx"     / "okx_spot.txt",
    ("okx", "futures"):     ALL_PAIRS_DIR / "okx"     / "okx_futures.txt",
}

# 12 комбинаций
COMBINATIONS = [
    ("okx", "spot", "mexc", "futures"),
    ("okx", "spot", "bybit", "futures"),
    ("okx", "spot", "binance", "futures"),
    ("mexc", "spot", "okx", "futures"),
    ("mexc", "spot", "bybit", "futures"),
    ("mexc", "spot", "binance", "futures"),
    ("bybit", "spot", "okx", "futures"),
    ("bybit", "spot", "mexc", "futures"),
    ("bybit", "spot", "binance", "futures"),
    ("binance", "spot", "okx", "futures"),
    ("binance", "spot", "mexc", "futures"),
    ("binance", "spot", "bybit", "futures"),
]


def normalize_pair(raw: str, exchange: str, market: str) -> str | None:
    s = raw.strip().upper()
    if not s:
        return None

    s = s.split()[0]

    if exchange == "okx":
        parts = s.split("-")
        if len(parts) >= 2:
            base, quote = parts[0], parts[1]
            if base and quote:
                return f"{base}{quote}"
        return None

    for sep in ("/", "-", "_"):
        if sep in s:
            parts = [p for p in s.split(sep) if p]
            if len(parts) >= 2:
                base, quote = parts[0], parts[1]
                return f"{base}{quote}"
            return None

    if ":" in s:
        s = s.split(":", 1)[0]

    if "-" in s:
        left, right = s.split("-", 1)
        if right in {"PERP", "SWAP", "FUT", "FUTURES"}:
            s = left

    return s or None


def load_set(exchange: str, market: str) -> set[str] | None:
    """Возвращает set пар или None, если файл не найден."""
    path = FILES[(exchange, market)]
    if not path.exists():
        return None

    out: set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            p = normalize_pair(line, exchange, market)
            if p:
                out.add(p)
    return out


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Загружаем все доступные списки
    sets: dict[tuple[str, str], set[str]] = {}
    print("=== INPUT LISTS ===")
    for (ex, mk), path in FILES.items():
        s = load_set(ex, mk)
        if s is not None:
            sets[(ex, mk)] = s
            print(f"  OK   {ex}_{mk:7s} : {len(s):>7d}  |  {path}")
        else:
            print(f"  SKIP {ex}_{mk:7s} : файл не найден  |  {path}")

    # Генерим пересечения только для тех комбинаций, где оба файла загружены
    print("\n=== OUTPUT INTERSECTIONS ===")
    done = 0
    skipped = 0
    for a_ex, a_mk, b_ex, b_mk in COMBINATIONS:
        name = f"{a_ex}_{a_mk}_{b_ex}_{b_mk}"

        missing = []
        if (a_ex, a_mk) not in sets:
            missing.append(f"{a_ex}_{a_mk}")
        if (b_ex, b_mk) not in sets:
            missing.append(f"{b_ex}_{b_mk}")

        if missing:
            print(f"  SKIP {name:30s} : нет данных для {', '.join(missing)}")
            skipped += 1
            continue

        inter = sets[(a_ex, a_mk)] & sets[(b_ex, b_mk)]
        out_path = OUT_DIR / f"{name}.txt"

        with out_path.open("w", encoding="utf-8") as w:
            for sym in sorted(inter):
                w.write(sym + "\n")

        print(f"  OK   {name:30s} : {len(inter):>7d}  ->  {out_path}")
        done += 1

    print(f"\nDone. Комбинаций: {done} выполнено, {skipped} пропущено.")


if __name__ == "__main__":
    main()
