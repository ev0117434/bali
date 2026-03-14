import json
from pathlib import Path
from urllib.request import urlopen, Request

# 🔥 УКАЖИ СВОЙ АБСОЛЮТНЫЙ ПУТЬ К ПАПКЕ
SAVE_DIR = Path("/root/siro/dictionaries/temp/all_pairs/bybit")

SPOT_FILE = "bybit_spot_all_temp.txt"
FUTURES_FILE = "bybit_futures_usdm_all_temp.txt"

BYBIT_SPOT_URL = "https://api.bybit.com/v5/market/instruments-info?category=spot"
BYBIT_LINEAR_URL = "https://api.bybit.com/v5/market/instruments-info?category=linear"


def fetch_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def extract_bybit_spot_symbols(data: dict) -> set[str]:
    pairs = set()
    for s in data.get("result", {}).get("list", []):
        if s.get("status") != "Trading":
            continue
        if s.get("quoteCoin") in ("USDT", "USDC"):
            pairs.add(s["symbol"])
    return pairs


def extract_bybit_linear_symbols(data: dict) -> set[str]:
    pairs = set()
    for s in data.get("result", {}).get("list", []):
        if s.get("status") != "Trading":
            continue
        # Linear = USDT/USDC-маржинальные фьючерсы
        if s.get("quoteCoin") in ("USDT", "USDC"):
            pairs.add(s["symbol"])
    return pairs


def save_pairs(pairs: set[str], filename: str) -> Path:
    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    file_path = SAVE_DIR / filename

    with file_path.open("w", encoding="utf-8") as f:
        for pair in sorted(pairs):
            f.write(pair + "\n")

    return file_path.resolve()


def main():
    print("Загружаем Bybit Spot пары...")
    spot_data = fetch_json(BYBIT_SPOT_URL)
    spot_pairs = extract_bybit_spot_symbols(spot_data)

    print("Загружаем Bybit Futures USDT-M пары...")
    futures_data = fetch_json(BYBIT_LINEAR_URL)
    futures_pairs = extract_bybit_linear_symbols(futures_data)

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
