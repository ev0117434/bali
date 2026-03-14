import json
from pathlib import Path
from urllib.request import urlopen, Request

SPOT_URL = "https://api.binance.com/api/v3/exchangeInfo"
FUTURES_USDM_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

# 🔥 УКАЖИ СВОЙ АБСОЛЮТНЫЙ ПУТЬ ЗДЕСЬ
SAVE_DIR = Path("/root/siro/dictionaries/temp/all_pairs/binance")


SPOT_FILE = "binance_spot_all_temp.txt"
FUTURES_FILE = "binance_futures_usdm_all_temp.txt"


def fetch_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def extract_usdt_usdc_symbols(exchange_info: dict) -> set[str]:
    pairs = set()
    for s in exchange_info.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        if s.get("quoteAsset") in ("USDT", "USDC"):
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
    print("Загружаем Spot пары...")
    spot_info = fetch_json(SPOT_URL)
    spot_pairs = extract_usdt_usdc_symbols(spot_info)

    print("Загружаем Futures USD-M пары...")
    futures_info = fetch_json(FUTURES_USDM_URL)
    futures_pairs = extract_usdt_usdc_symbols(futures_info)

    spot_path = save_pairs(spot_pairs, SPOT_FILE)
    futures_path = save_pairs(futures_pairs, FUTURES_FILE)

    print("\nГотово ✅")
    print(f"Spot пар: {len(spot_pairs)}")
    print(f"Futures пар: {len(futures_pairs)}")
    print(f"\nФайлы сохранены:")
    print(spot_path)
    print(futures_path)


if __name__ == "__main__":
    main()
