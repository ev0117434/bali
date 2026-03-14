#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path

BASE_DIR = Path("/root/siro/dictionaries")
IN_DIR = BASE_DIR / "combination"          # 12 файлов направлений
OUT_BASE_DIR = BASE_DIR / "subscribe"     # тут будут папки бирж

EXCHANGES = ("binance", "bybit", "mexc", "okx")
MARKETS = ("spot", "futures")

def read_pairs(path: Path) -> set[str]:
    s: set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            p = line.strip().upper()
            if p:
                s.add(p)
    return s

def main() -> None:
    if not IN_DIR.exists():
        raise FileNotFoundError(f"Не найдена папка с направлениями (12): {IN_DIR}")

    # Корзины под итоговые списки
    buckets: dict[tuple[str, str], set[str]] = {
        (ex, mk): set() for ex in EXCHANGES for mk in MARKETS
    }

    files = sorted(IN_DIR.glob("*.txt"))
    print(f"Found {len(files)} direction files in: {IN_DIR}")

    for fp in files:
        name = fp.stem  # без .txt
        parts = name.split("_")
        if len(parts) != 4:
            print(f"[SKIP] unexpected filename: {fp.name}")
            continue

        ex1, mk1, ex2, mk2 = parts
        if ex1 not in EXCHANGES or ex2 not in EXCHANGES or mk1 not in MARKETS or mk2 not in MARKETS:
            print(f"[SKIP] unknown exchange/market in filename: {fp.name}")
            continue

        pairs = read_pairs(fp)

        buckets[(ex1, mk1)].update(pairs)
        buckets[(ex2, mk2)].update(pairs)

    print("\n=== OUTPUT subscribe/<exchange>/ ===")
    for ex in EXCHANGES:
        ex_dir = OUT_BASE_DIR / ex
        ex_dir.mkdir(parents=True, exist_ok=True)

        for mk in MARKETS:
            out_path = ex_dir / f"{ex}_{mk}.txt"
            data = sorted(buckets[(ex, mk)])

            with out_path.open("w", encoding="utf-8") as w:
                for p in data:
                    w.write(p + "\n")

            print(f"{ex}/{ex}_{mk:7s}: {len(data):>7d}  ->  {out_path}")

    print("\nDone.")

if __name__ == "__main__":
    main()
