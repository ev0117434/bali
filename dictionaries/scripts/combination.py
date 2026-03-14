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

# 12 комбинаций (как ты перечислил)
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
    """
    Приводим пары к единому виду: BASEQUOTE (например BTCUSDT).
    Нюанс OKX:
      - spot:    BTC-USDT      -> BTCUSDT
      - futures: ALLO-USDT-SWAP-> ALLOUSDT
    Для остальных бирж тоже пытаемся аккуратно нормализовать (/ - _ :), если встретится.
    """
    s = raw.strip().upper()
    if not s:
        return None

    # выкидываем всё после пробела/табов (если в строке ещё что-то)
    s = s.split()[0]

    # OKX: всегда BASE-QUOTE-... (spot: BASE-QUOTE, futures: BASE-QUOTE-SWAP)
    if exchange == "okx":
        parts = s.split("-")
        if len(parts) >= 2:
            base, quote = parts[0], parts[1]
            if base and quote:
                return f"{base}{quote}"
        return None

    # Универсальная нормализация для остальных:
    # - Если есть разделитель, берём первые 2 токена как base/quote
    # - Иначе оставляем как есть (например BTCUSDT уже норм)
    for sep in ("/", "-", "_"):
        if sep in s:
            parts = [p for p in s.split(sep) if p]
            if len(parts) >= 2:
                base, quote = parts[0], parts[1]
                return f"{base}{quote}"
            return None

    # Иногда встречается формат типа BTCUSDT:USDT или BTCUSDT-PERP и т.п.
    # Берём часть до ':' и до последнего '-' если там PERP/SWAP и пр.
    if ":" in s:
        s = s.split(":", 1)[0]

    # Если вдруг попался хвост через '-' (редко у не-okx), режем его
    # но только если это явно хвост типа PERP/SWAP/FUT
    if "-" in s:
        left, right = s.split("-", 1)
        if right in {"PERP", "SWAP", "FUT", "FUTURES"}:
            s = left

    return s or None


def load_set(exchange: str, market: str) -> set[str]:
    path = FILES[(exchange, market)]
    if not path.exists():
        raise FileNotFoundError(f"Не найден файл: {path}")

    out: set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            p = normalize_pair(line, exchange, market)
            if p:
                out.add(p)
    return out


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Загружаем 8 списков в память (как множества)
    sets: dict[tuple[str, str], set[str]] = {}
    print("=== INPUT LISTS COUNTS (unique after mapping) ===")
    for (ex, mk), path in FILES.items():
        s = load_set(ex, mk)
        sets[(ex, mk)] = s
        print(f"{ex}_{mk:7s} : {len(s):>7d}  |  {path}")

    # Генерим 12 пересечений
    print("\n=== OUTPUT INTERSECTIONS COUNTS ===")
    for a_ex, a_mk, b_ex, b_mk in COMBINATIONS:
        a = sets[(a_ex, a_mk)]
        b = sets[(b_ex, b_mk)]
        inter = a & b

        name = f"{a_ex}_{a_mk}_{b_ex}_{b_mk}"
        out_path = OUT_DIR / f"{name}.txt"

        with out_path.open("w", encoding="utf-8") as w:
            for sym in sorted(inter):
                w.write(sym + "\n")

        print(f"{name:30s} : {len(inter):>7d}  ->  {out_path}")

    print("\nDone.")

if __name__ == "__main__":
    main()
