#!/usr/bin/env python3
import os

BASE_DIR = "/root/siro/dictionaries/subscribe"
EXCHANGES = ["okx", "mexc", "bybit", "binance"]
MARKETS = ["spot", "futures"]

OUTPUT_FILE = "/root/siro/dictionaries/subscribe/all_symbols.txt"


def iter_input_files():
    for exch in EXCHANGES:
        for market in MARKETS:
            yield exch, market, os.path.join(BASE_DIR, exch, f"{exch}_{market}.txt")


def merge_unique_symbols():
    seen = set()
    result = []
    stats = []

    for exch, market, path in iter_input_files():
        if not os.path.exists(path):
            stats.append((path, "MISSING", 0, 0))
            print(f"[MISSING] {path}")
            continue

        total_lines = 0
        added_here = 0

        print(f"[READING] {path}")
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                symbol = line.strip()
                if not symbol:
                    continue
                total_lines += 1
                if symbol not in seen:
                    seen.add(symbol)
                    result.append(symbol)
                    added_here += 1

        stats.append((path, "OK", total_lines, added_here))

    return result, stats


def main():
    symbols, stats = merge_unique_symbols()

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(symbols) + "\n")

    print("\n=========== SUMMARY ===========")
    for path, status, total, added in stats:
        if status == "OK":
            print(f"[OK] {path}")
            print(f"     lines read: {total}")
            print(f"     new symbols added: {added}")
        else:
            print(f"[MISSING] {path}")

    print("\n=========== RESULT ===========")
    print(f"Total unique symbols: {len(symbols)}")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
