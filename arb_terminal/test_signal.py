#!/usr/bin/env python3
"""
test_signal.py — Publish fake signals to Redis for terminal testing.

Usage:
    # JSON signal (default):
    python arb_terminal/test_signal.py

    # CSV signal:
    python arb_terminal/test_signal.py --format csv

    # Custom parameters:
    python arb_terminal/test_signal.py --buy-exchange binance --sell-exchange bybit \
        --symbol BTCUSDT --spread 1.5

    # Repeat every N seconds:
    python arb_terminal/test_signal.py --repeat 5
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import redis
import arb_terminal.config as cfg


PRESETS = {
    "THEUSDT": {"buy_ask": 0.2338, "sell_bid": 0.2362, "buy_ask_qty": 5000, "sell_bid_qty": 3000},
    "BTCUSDT": {"buy_ask": 45000.1, "sell_bid": 45451.5, "buy_ask_qty": 0.532, "sell_bid_qty": 1.2},
    "ETHUSDT": {"buy_ask": 2500.5, "sell_bid": 2518.0, "buy_ask_qty": 10.0, "sell_bid_qty": 8.5},
    "SOLUSDT": {"buy_ask": 180.25, "sell_bid": 181.50, "buy_ask_qty": 200.0, "sell_bid_qty": 150.0},
}


def make_json_signal(
    symbol: str,
    buy_exchange: str,
    sell_exchange: str,
    buy_ask: float,
    sell_bid: float,
    buy_ask_qty: float,
    sell_bid_qty: float,
    direction: str = "A",
) -> str:
    spread_pct = (sell_bid / buy_ask - 1) * 100
    payload = {
        "symbol": symbol,
        "direction": direction,
        "buy_exchange": buy_exchange,
        "buy_market": "spot",
        "buy_ask": str(buy_ask),
        "buy_ask_qty": str(buy_ask_qty),
        "sell_exchange": sell_exchange,
        "sell_market": "futures",
        "sell_bid": str(sell_bid),
        "sell_bid_qty": str(sell_bid_qty),
        "spread_pct": round(spread_pct, 4),
        "ts_signal": int(time.time() * 1000),
    }
    return json.dumps(payload)


def make_csv_signal(
    symbol: str,
    buy_exchange: str,
    sell_exchange: str,
    buy_ask: float,
    sell_bid: float,
) -> str:
    spread_pct = round((sell_bid / buy_ask - 1) * 100, 4)
    ts = int(time.time() * 1000)
    return f"{buy_exchange},{sell_exchange}.{symbol},{buy_ask},{sell_bid},{spread_pct},{ts}"


def publish(r: redis.Redis, channel: str, message: str) -> None:
    listeners = r.publish(channel, message)
    print(f"Published to '{channel}' ({listeners} listeners):")
    print(f"  {message[:120]}{'...' if len(message) > 120 else ''}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish test signals to Redis")
    parser.add_argument("--format", choices=["json", "csv"], default="json")
    parser.add_argument("--buy-exchange", default="binance")
    parser.add_argument("--sell-exchange", default="gate")
    parser.add_argument("--symbol", default="THEUSDT")
    parser.add_argument("--spread", type=float, default=None, help="Override spread %")
    parser.add_argument("--repeat", type=float, default=None, help="Repeat every N seconds")
    parser.add_argument("--channel", default=cfg.REDIS_CHANNEL)
    parser.add_argument("--host", default=cfg.REDIS_HOST)
    parser.add_argument("--port", type=int, default=cfg.REDIS_PORT)
    args = parser.parse_args()

    r = redis.Redis(host=args.host, port=args.port, decode_responses=True)
    try:
        r.ping()
    except Exception as exc:
        print(f"Redis connection failed: {exc}")
        sys.exit(1)

    preset = PRESETS.get(args.symbol.upper(), PRESETS["THEUSDT"])
    buy_ask = preset["buy_ask"]
    sell_bid = preset["sell_bid"]

    # Apply custom spread if given
    if args.spread is not None:
        sell_bid = buy_ask * (1 + args.spread / 100)

    buy_ask_qty = preset["buy_ask_qty"]
    sell_bid_qty = preset["sell_bid_qty"]

    def send_one():
        if args.format == "json":
            msg = make_json_signal(
                symbol=args.symbol.upper(),
                buy_exchange=args.buy_exchange,
                sell_exchange=args.sell_exchange,
                buy_ask=buy_ask,
                sell_bid=sell_bid,
                buy_ask_qty=buy_ask_qty,
                sell_bid_qty=sell_bid_qty,
            )
        else:
            msg = make_csv_signal(
                symbol=args.symbol.upper(),
                buy_exchange=args.buy_exchange,
                sell_exchange=args.sell_exchange,
                buy_ask=buy_ask,
                sell_bid=sell_bid,
            )
        publish(r, args.channel, msg)

    if args.repeat:
        print(f"Sending every {args.repeat}s — Ctrl+C to stop")
        while True:
            send_one()
            time.sleep(args.repeat)
    else:
        send_one()


if __name__ == "__main__":
    main()
