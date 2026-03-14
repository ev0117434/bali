import json
from pathlib import Path
from urllib.request import urlopen, Request

# 🔥 УКАЖИ СВОЙ АБСОЛЮТНЫЙ ПУТЬ К ПАПКЕ
SAVE_DIR = Path("/root/siro/dictionaries/temp/all_pairs/okx")

SPOT_FILE = "okx_spot_all_temp.txt"
FUTURES_FILE = "okx_futures_usdm_all_temp.txt"

OKX_SPOT_URL = "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
OKX_SWAP_URL = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
OKX_FUTURES_URL = "https://www.okx.com/api/v5/public/instruments?instType=FUTURES"


def fetch_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def extract_okx_spot_usdt_usdc(data: dict) -> set[str]:
    pairs = set()
    for it in data.get("data", []):
        # trading state
        if (it.get("state") or "").lower() != "live":
            continue
        # quote currency filter
        if (it.get("quoteCcy") or "").upper() not in ("USDT", "USDC"):
            continue
        inst_id = it.get("instId")
        if inst_id:
            pairs.add(inst_id)
    return pairs


def extract_okx_deriv_usdt_usdc(data: dict) -> set[str]:
    pairs = set()
    for it in data.get("data", []):
        if (it.get("state") or "").lower() != "live":
            continue
        # USD-M аналог на OKX: settleCcy = USDT/USDC
        if (it.get("settleCcy") or "").upper() not in ("USDT", "USDC"):
            continue
        inst_id = it.get("instId")
        if inst_id:
            pairs.add(inst_id)
    return pairs


def save_pairs(pairs: set[str], filename: str) -> Path:
    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    file_path = SAVE_DIR / filename
    with file_path.open("w", encoding="utf-8") as f:
        for p in sorted(pairs):
            f.write(p + "\n")
    return file_path.resolve()


def main():
    print("Загружаем OKX Spot инструменты...")
    spot_data = fetch_json(OKX_SPOT_URL)
    spot_pairs = extract_okx_spot_usdt_usdc(spot_data)

    print("Загружаем OKX SWAP (perpetual) инструменты...")
    swap_data = fetch_json(OKX_SWAP_URL)
    swap_pairs = extract_okx_deriv_usdt_usdc(swap_data)

    print("Загружаем OKX FUTURES (dated) инструменты...")
    fut_data = fetch_json(OKX_FUTURES_URL)
    fut_pairs = extract_okx_deriv_usdt_usdc(fut_data)

    # Один файл для "usd-m": SWAP + FUTURES
    usdm_pairs = swap_pairs | fut_pairs

    spot_path = save_pairs(spot_pairs, SPOT_FILE)
    futures_path = save_pairs(usdm_pairs, FUTURES_FILE)

    print("\nГотово ✅")
    print(f"Spot пар: {len(spot_pairs)}")
    print(f"USD-M деривативов (SWAP+FUTURES): {len(usdm_pairs)}")
    print("\nФайлы сохранены по абсолютным путям:")
    print(spot_path)
    print(futures_path)


if __name__ == "__main__":
    main()
