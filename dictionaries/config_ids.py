#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path

# ====== ВХОДНЫЕ ФАЙЛЫ (ЗАШИТЫЕ ПУТИ) ======
ALL_SYMBOLS_TXT = Path("/root/siro/dictionaries/subscribe/all_symbols.txt")
SOURCES_TXT     = Path("/root/siro/dictionaries/all_pairs/source.txt")

# ====== ВЫХОДНАЯ ПАПКА ======
OUT_DIR = Path("/root/siro/dictionaries/configs")

# ====== ВЫХОДНЫЕ ФАЙЛЫ (ТОЛЬКО TSV) ======
SYMBOLS_TSV = OUT_DIR / "symbols.tsv"   # symbol_id \t SYMBOL
SOURCES_TSV = OUT_DIR / "sources.tsv"   # source_id \t SOURCE


def read_lines_unique_keep_order(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"file not found: {path}")

    items: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        items.append(s)

    # dedup, keep order
    seen = set()
    out: list[str] = []
    for s in items:
        if s not in seen:
            out.append(s)
            seen.add(s)
    return out


def load_existing_symbols_tsv(path: Path) -> dict[str, int]:
    """
    symbols.tsv формат:
      <id>\t<SYMBOL>
    -> mapping SYMBOL -> id

    Если файла нет — вернём пустую мапу (все id начнутся с 0).
    """
    if not path.exists():
        return {}

    mapping: dict[str, int] = {}
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        s = line.strip()
        if not s:
            continue
        parts = s.split("\t")
        if len(parts) != 2:
            raise ValueError(f"bad line in {path} at {n}: expected '<id>\\t<SYMBOL>', got: {line!r}")

        sid_str, sym = parts[0].strip(), parts[1].strip()
        if not sid_str.isdigit():
            raise ValueError(f"bad id in {path} at {n}: {sid_str!r}")

        sid = int(sid_str)
        if sym in mapping and mapping[sym] != sid:
            raise ValueError(f"duplicate symbol with different id in {path} at {n}: {sym}")

        mapping[sym] = sid

    return mapping


def dump_symbols_tsv(path: Path, sym_to_id: dict[str, int]) -> None:
    # строго в порядке id: 0..N
    pairs = sorted(sym_to_id.items(), key=lambda kv: kv[1])  # (SYMBOL, id)
    lines = [f"{sid}\t{sym}" for sym, sid in pairs]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def dump_sources_tsv(path: Path, sources: list[str]) -> None:
    lines = [f"{i}\t{name}" for i, name in enumerate(sources)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Sources: id по порядку в source.txt
    sources = read_lines_unique_keep_order(SOURCES_TXT)
    dump_sources_tsv(SOURCES_TSV, sources)

    # 2) Symbols: stable_append через symbols.tsv (никакого JSON)
    symbols = read_lines_unique_keep_order(ALL_SYMBOLS_TXT)

    old = load_existing_symbols_tsv(SYMBOLS_TSV)
    sym_to_id = dict(old)

    next_id = 0 if not old else (max(old.values()) + 1)

    for s in symbols:
        if s not in sym_to_id:
            sym_to_id[s] = next_id
            next_id += 1

    # предупреждение: если символ убрали из all_symbols.txt — мы НЕ удаляем его из symbols.tsv,
    # чтобы старые id никогда не менялись.
    removed = sorted(set(old.keys()) - set(symbols))
    if removed:
        print(
            f"[warn] removed from all_symbols.txt, but kept in symbols.tsv for stable ids: "
            f"{removed[:10]}" + (" ..." if len(removed) > 10 else "")
        )

    dump_symbols_tsv(SYMBOLS_TSV, sym_to_id)

    print("OK")
    print(f"symbols from: {ALL_SYMBOLS_TXT} ({len(symbols)} in txt, {len(sym_to_id)} total ids)")
    print(f"sources from: {SOURCES_TXT} ({len(sources)})")
    print(f"out dir: {OUT_DIR}")
    print(f"wrote: {SYMBOLS_TSV}")
    print(f"wrote: {SOURCES_TSV}")


if __name__ == "__main__":
    main()
