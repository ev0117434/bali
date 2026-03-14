import json
from pathlib import Path
from urllib.request import urlopen, Request

# 🔥 УКАЖИ СВОЙ АБСОЛЮТНЫЙ ПУТЬ К ПАПКЕ
SAVE_DIR = Path("/root/siro/dictionaries/temp/all_pairs/mexc")

SPOT_FILE = "mexc_spot_all_temp.txt"
FUTURES_FILE = "mexc_futures_usdm_all_temp.txt"

# ✅ Самый надёжный список доступных SPOT пар у MEXC
MEXC_SPOT_DEFAULT_SYMBOLS_URL = "https://api.mexc.com/api/v3/defaultSymbols"

# Futures (USDT-M) contracts list
MEXC_FUTURES_URL = "https://contract.mexc.com/api/v1/contract/detail"


def fetch_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8")


def extract_mexc_spot_usdt_usdc_from_defaultsymbols() -> set[str]:
    """
    /api/v3/defaultSymbols обычно возвращает JSON-массив строк, например:
    ["BTCUSDT","ETHUSDT",...]
    Иногда может быть {"data":[...]} — на всякий случай обработаем оба варианта.
    """
    raw = fetch_text(MEXC_SPOT_DEFAULT_SYMBOLS_URL).strip()
    data = json.loads(raw)

    if isinstance(data, list):
        symbols = data
    elif isinstance(data, dict):
        symbols = data.get("data", []) or data.get("symbols", []) or []
    else:
        symbols = []

    out = set()
    for s in symbols:
        if not isinstance(s, str):
            continue
        s_up = s.upper()
        if s_up.endswith("USDT") or s_up.endswith("USDC"):
            out.add(s_up)
    return out


def extract_mexc_futures_usdt_usdc(data: dict) -> set[str]:
    pairs = set()
    items = data.get("data", [])
    if not isinstance(items, list):
        return pairs

    for c in items:
        # статус у контрактов бывает разный по полям/типам
        state = c.get("state")
        status = c.get("status")

        is_trading = True
        if state is not None:
            is_trading = (state == 0)
        elif status is not None:
            is_trading = (status == 1)

        if not is_trading:
            continue

        quote = (c.get("quoteCoin") or c.get("quote_coin") or "").upper()
        if quote not in ("USDT", "USDC"):
            continue

        sym = c.get("symbol") or c.get("symbolName") or c.get("contractCode")
        if not sym:
            continue

        sym = sym.replace("_", "").upper()
        pairs.add(sym)

    return pairs


def save_pairs(pairs: set[str], filename: str) -> Path:
    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    file_path = SAVE_DIR / filename
    with file_path.open("w", encoding="utf-8") as f:
        for p in sorted(pairs):
            f.write(p + "\n")
    return file_path.resolve()


def main():
    print("Загружаем MEXC Spot пары (defaultSymbols)...")
    spot_pairs = extract_mexc_spot_usdt_usdc_from_defaultsymbols()

    print("Загружаем MEXC Futures (USDT-M) пары...")
    futures_info = fetch_json(MEXC_FUTURES_URL)
    futures_pairs = extract_mexc_futures_usdt_usdc(futures_info)

    spot_path = save_pairs(spot_pairs, SPOT_FILE)
    futures_path = save_pairs(futures_pairs, FUTURES_FILE)

    print("\nГотово ✅")
    print(f"Spot пар: {len(spot_pairs)}")
    print(f"Futures пар: {len(futures_pairs)}")
    print("\nФайлы сохранены по абсолютным путям:")
    print(spot_path)
    print(futures_path)


if __name__ == "__main__":
    main()
