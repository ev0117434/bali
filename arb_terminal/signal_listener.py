"""
Signal listener — Redis pub/sub + JSON/CSV parser.

Signal formats
--------------
JSON:
    {"symbol":"THEUSDT","direction":"A","buy_exchange":"binance","buy_market":"spot",
     "buy_ask":"0.2338","buy_ask_qty":"5000","sell_exchange":"gate","sell_market":"futures",
     "sell_bid":"0.2362","sell_bid_qty":"3000","spread_pct":1.0265,"ts_signal":1773590375286}

CSV (header-less):
    binance,gate.THEUSDT,0.2338,0.2362,1.0265,1773590375286
    columns: buy_exchange, sell_exchange.SYMBOL, buy_ask, sell_bid, spread_pct, ts_signal
"""

import json
import sys
import time
from dataclasses import dataclass
from typing import Generator, Optional

import redis

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EXCHANGE_ALIASES: dict[str, str] = {
    "gate": "gateio",
    "gate.io": "gateio",
}

# Quote currencies tried in order (longest first to avoid partial matches)
_QUOTES = ["USDT", "USDC", "BUSD", "TUSD", "DAI", "BTC", "ETH", "BNB"]


def normalize_exchange(name: str) -> str:
    """'gate' → 'gateio', everything else lowercased as-is."""
    return _EXCHANGE_ALIASES.get(name.lower(), name.lower())


def raw_symbol_to_ccxt(raw: str) -> str:
    """'THEUSDT' → 'THE/USDT'.  Falls back to raw if no known quote is found."""
    raw = raw.upper()
    for quote in _QUOTES:
        if raw.endswith(quote) and len(raw) > len(quote):
            base = raw[: -len(quote)]
            if base:
                return f"{base}/{quote}"
    return raw  # unknown format — return as-is


# ---------------------------------------------------------------------------
# Signal dataclass
# ---------------------------------------------------------------------------


@dataclass
class Signal:
    symbol: str         # ccxt format:  THE/USDT
    raw_symbol: str     # original:     THEUSDT
    direction: str      # A-L (or '?' from CSV)

    buy_exchange: str   # normalised: binance / bybit / gateio / okx
    buy_market: str     # spot / futures
    buy_ask: float
    buy_ask_qty: float

    sell_exchange: str
    sell_market: str
    sell_bid: float
    sell_bid_qty: float

    spread_pct: float   # as given by scanner, e.g. 1.0265  (means +1.0265%)
    ts_signal: int      # Unix ms

    # ------------------------------------------------------------------
    @property
    def age_sec(self) -> float:
        return (time.time() * 1000 - self.ts_signal) / 1000

    @property
    def market_spread_pct(self) -> float:
        """Recalculate spread from ask/bid."""
        if self.buy_ask <= 0:
            return 0.0
        return (self.sell_bid / self.buy_ask - 1) * 100


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def _parse_json(raw: str) -> Optional[Signal]:
    try:
        d = json.loads(raw)
        raw_sym = str(d["symbol"])
        return Signal(
            symbol=raw_symbol_to_ccxt(raw_sym),
            raw_symbol=raw_sym,
            direction=str(d.get("direction", "?")),
            buy_exchange=normalize_exchange(str(d["buy_exchange"])),
            buy_market=str(d.get("buy_market", "spot")),
            buy_ask=float(d["buy_ask"]),
            buy_ask_qty=float(d.get("buy_ask_qty", 0) or 0),
            sell_exchange=normalize_exchange(str(d["sell_exchange"])),
            sell_market=str(d.get("sell_market", "futures")),
            sell_bid=float(d["sell_bid"]),
            sell_bid_qty=float(d.get("sell_bid_qty", 0) or 0),
            spread_pct=float(d["spread_pct"]),
            ts_signal=int(d["ts_signal"]),
        )
    except (json.JSONDecodeError, KeyError, ValueError, TypeError):
        return None


def _parse_csv(raw: str) -> Optional[Signal]:
    """
    Format: buy_exchange,sell_exchange.SYMBOL,buy_ask,sell_bid,spread_pct,ts_signal
    Example: binance,gate.THEUSDT,0.2338,0.2362,1.0265,1773590375286
    """
    try:
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) < 6:
            return None

        buy_exchange = parts[0]
        sell_part = parts[1]  # e.g. "gate.THEUSDT"

        dot_idx = sell_part.index(".")
        sell_exchange = sell_part[:dot_idx]
        raw_sym = sell_part[dot_idx + 1:]

        return Signal(
            symbol=raw_symbol_to_ccxt(raw_sym),
            raw_symbol=raw_sym,
            direction="?",
            buy_exchange=normalize_exchange(buy_exchange),
            buy_market="spot",
            buy_ask=float(parts[2]),
            buy_ask_qty=0.0,
            sell_exchange=normalize_exchange(sell_exchange),
            sell_market="futures",
            sell_bid=float(parts[3]),
            sell_bid_qty=0.0,
            spread_pct=float(parts[4]),
            ts_signal=int(parts[5]),
        )
    except (ValueError, IndexError):
        return None


def parse_signal(raw: str) -> Optional[Signal]:
    """Try JSON first, then CSV. Returns None if both fail."""
    raw = raw.strip()
    sig = _parse_json(raw)
    if sig is None:
        sig = _parse_csv(raw)
    return sig


# ---------------------------------------------------------------------------
# SignalListener
# ---------------------------------------------------------------------------


class SignalListener:
    def __init__(self, host: str, port: int, channel: str, password: str = ""):
        self._host = host
        self._port = port
        self._channel = channel
        self._password = password
        self._client: Optional[redis.Redis] = None
        self._pubsub = None

    def connect(self) -> None:
        kwargs: dict = {
            "host": self._host,
            "port": self._port,
            "decode_responses": True,
        }
        if self._password:
            kwargs["password"] = self._password
        self._client = redis.Redis(**kwargs)
        self._client.ping()
        self._pubsub = self._client.pubsub(ignore_subscribe_messages=True)
        self._pubsub.subscribe(self._channel)

    def listen(self) -> Generator[Signal, None, None]:
        """
        Blocking generator. Yields parsed Signal objects.
        Uses get_message(timeout=1) so Ctrl+C (KeyboardInterrupt) is
        always delivered within ~1 second.
        """
        assert self._pubsub is not None, "Call connect() first"
        while True:
            message = self._pubsub.get_message(
                ignore_subscribe_messages=True, timeout=1.0
            )
            if message and message.get("type") == "message":
                data = message["data"]
                sig = parse_signal(data)
                if sig is None:
                    print(
                        f"[WARN] Cannot parse signal: {data!r}", file=sys.stderr
                    )
                else:
                    yield sig

    def close(self) -> None:
        try:
            if self._pubsub:
                self._pubsub.unsubscribe()
        except Exception:
            pass
        try:
            if self._client:
                self._client.close()
        except Exception:
            pass
