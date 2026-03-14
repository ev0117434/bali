#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path

BASE_DIR = Path("/root/siro/dictionaries")
OKX_SUBSCRIBE_DIR = BASE_DIR / "subscribe" / "okx"
IN_SPOT = OKX_SUBSCRIBE_DIR / "okx_spot.txt"
IN_FUT = OKX_SUBSCRIBE_DIR / "okx_futures.txt"

OUT_DIR = OKX_SUBSCRIBE_DIR / "with_mapping"
OUT_SPOT = OUT_DIR / "okx_spot.txt"
OUT_FUT = OUT_DIR / "okx_futures.txt"

# Чем больше/длиннее список котировок — тем точнее обратный маппинг.
# Важно: сортируем по длине, чтобы сначала матчились типа "USDT", "USDC", "FDUSD", а не "USD".
COMMON_QUOTES = sorted({
    "USDT","USDC","BUSD","FDUSD","TUSD","DAI","USD",
    "BTC","ETH","BNB","SOL","XRP","TON","TRX","DOGE","ADA","AVAX","DOT","LTC","BCH","LINK","MATIC",
    "EUR","GBP","JPY","KRW","TRY","BRL","AUD","CAD","CHF","HKD","SGD","INR","IDR","RUB","UAH","NGN","ZAR",
    "MXN","ARS","COP","CLP","PEN","VND","THB","MYR","PHP",
}, key=len, reverse=True)

def read_pairs(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Не найден файл: {path}")
    out = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip().upper()
            if s:
                out.append(s)
    return out

def split_base_quote(sym: str) -> tuple[str, str] | None:
    # sym вида BTCUSDT -> ("BTC","USDT")
    for q in COMMON_QUOTES:
        if sym.endswith(q) and len(sym) > len(q):
            base = sym[:-len(q)]
            return base, q
    return None

def to_okx_spot(sym: str) -> str | None:
    bq = split_base_quote(sym)
    if not bq:
        return None
    base, quote = bq
    return f"{base}-{quote}"

def to_okx_futures(sym: str) -> str | None:
    bq = split_base_quote(sym)
    if not bq:
        return None
    base, quote = bq
    return f"{base}-{quote}-SWAP"

def write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as w:
        for s in lines:
            w.write(s + "\n")

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    spot_in = read_pairs(IN_SPOT)
    fut_in = read_pairs(IN_FUT)

    spot_out_set = set()
    fut_out_set = set()

    spot_unmapped = []
    fut_unmapped = []

    for s in spot_in:
        mapped = to_okx_spot(s)
        if mapped:
            spot_out_set.add(mapped)
        else:
            spot_unmapped.append(s)

    for s in fut_in:
        mapped = to_okx_futures(s)
        if mapped:
            fut_out_set.add(mapped)
        else:
            fut_unmapped.append(s)

    spot_out = sorted(spot_out_set)
    fut_out = sorted(fut_out_set)

    write_lines(OUT_SPOT, spot_out)
    write_lines(OUT_FUT, fut_out)

    print("=== OKX WITH MAPPING ===")
    print(f"Input  spot   : {len(set(spot_in))} unique  | {IN_SPOT}")
    print(f"Output spot   : {len(spot_out)} mapped  | {OUT_SPOT}")
    print(f"Input  futures: {len(set(fut_in))} unique  | {IN_FUT}")
    print(f"Output futures: {len(fut_out)} mapped  | {OUT_FUT}")

    # Если какие-то пары не удалось обратно распарсить (редкие котировки)
    if spot_unmapped or fut_unmapped:
        print("\n[WARN] Unmapped symbols (add quote to COMMON_QUOTES if needed):")
        if spot_unmapped:
            print(f"  spot   unmapped: {len(spot_unmapped)} (examples: {spot_unmapped[:10]})")
        if fut_unmapped:
            print(f"  futures unmapped: {len(fut_unmapped)} (examples: {fut_unmapped[:10]})")

    print("\nDone.")

if __name__ == "__main__":
    main()
