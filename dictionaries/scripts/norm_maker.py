#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from pathlib import Path

ROOT = Path("/root/siro/dictionaries")

TARGET_DIRS = [
    ROOT / "combination",
    ROOT / "subscribe" / "binance",
    ROOT / "subscribe" / "mexc",
    ROOT / "subscribe" / "okx",
    ROOT / "subscribe" / "bybit",
]

def is_ascii_line(line: str) -> bool:
    # True если в строке только ASCII (0..127)
    return all(ord(ch) < 128 for ch in line)

def clean_file(path: Path, make_backup: bool = True) -> tuple[int, int]:
    """
    Удаляет строки с non-ASCII символами.
    Возвращает (сколько_строк_было, сколько_удалили).
    """
    try:
        # errors="replace": чтобы не падать на битой кодировке, а заменить непонятные байты на �
        # (он non-ASCII) => строка будет удалена, что обычно и нужно для "очистки"
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        print(f"[ERR] Не смог прочитать {path}: {e}", file=sys.stderr)
        return (0, 0)

    lines = text.splitlines(keepends=True)
    kept = []
    removed = 0

    for ln in lines:
        if is_ascii_line(ln):
            kept.append(ln)
        else:
            removed += 1

    if removed == 0:
        return (len(lines), 0)

    if make_backup:
        backup_path = path.with_suffix(path.suffix + ".bak")
        try:
            if not backup_path.exists():
                backup_path.write_text(text, encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"[ERR] Не смог сделать backup для {path}: {e}", file=sys.stderr)
            return (len(lines), 0)

    try:
        path.write_text("".join(kept), encoding="utf-8", errors="strict")
    except Exception as e:
        print(f"[ERR] Не смог записать {path}: {e}", file=sys.stderr)
        return (len(lines), 0)

    return (len(lines), removed)

def iter_txt_files(base: Path):
    if not base.exists():
        return
    for p in base.rglob("*.txt"):
        if p.is_file():
            yield p

def main():
    total_files = 0
    changed_files = 0
    total_removed = 0

    for d in TARGET_DIRS:
        for f in iter_txt_files(d):
            total_files += 1
            before, removed = clean_file(f, make_backup=True)
            if removed > 0:
                changed_files += 1
                total_removed += removed
                print(f"[OK] {f}  (удалено строк: {removed})")

    print("\n--- Итог ---")
    print(f"Файлов найдено: {total_files}")
    print(f"Файлов изменено: {changed_files}")
    print(f"Удалено строк всего: {total_removed}")

if __name__ == "__main__":
    main()
