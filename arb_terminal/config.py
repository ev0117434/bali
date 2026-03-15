"""Configuration — loaded from .env or environment variables."""

import os
from pathlib import Path

from dotenv import load_dotenv

_HERE = Path(__file__).parent
load_dotenv(_HERE / ".env")
load_dotenv(_HERE.parent / ".env")  # also try project root

# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------
REDIS_HOST: str = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "")
REDIS_CHANNEL: str = os.getenv("REDIS_CHANNEL", "ch:spread_signals")

# ---------------------------------------------------------------------------
# Trading defaults  (overrideable at startup)
# ---------------------------------------------------------------------------
POSITION_SIZE_USDT: float = float(os.getenv("POSITION_SIZE_USDT", "100"))
SPREAD_REDUCTION: float = float(os.getenv("SPREAD_REDUCTION", "0.30"))
ORDER_POLL_INTERVAL: float = float(os.getenv("ORDER_POLL_INTERVAL", "1.0"))
PNL_POLL_INTERVAL: float = float(os.getenv("PNL_POLL_INTERVAL", "2.0"))
SIGNAL_MAX_AGE_SEC: float = float(os.getenv("SIGNAL_MAX_AGE_SEC", "30"))
ORDER_TIMEOUT_SEC: float = float(os.getenv("ORDER_TIMEOUT_MIN", "5")) * 60

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR: Path = Path(os.getenv("LOG_DIR", str(_HERE / "logs")))
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Exchange API keys
# ---------------------------------------------------------------------------
BINANCE_TESTNET: bool = os.getenv("BINANCE_TESTNET", "false").lower() == "true"
BYBIT_TESTNET: bool = os.getenv("BYBIT_TESTNET", "false").lower() == "true"

EXCHANGE_KEYS: dict = {
    "binance": {
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": os.getenv("BINANCE_SECRET", ""),
        "testnet": BINANCE_TESTNET,
    },
    "bybit": {
        "apiKey": os.getenv("BYBIT_API_KEY", ""),
        "secret": os.getenv("BYBIT_SECRET", ""),
        "testnet": BYBIT_TESTNET,
    },
    "gateio": {
        "apiKey": os.getenv("GATE_API_KEY", ""),
        "secret": os.getenv("GATE_SECRET", ""),
    },
    "okx": {
        "apiKey": os.getenv("OKX_API_KEY", ""),
        "secret": os.getenv("OKX_SECRET", ""),
        "password": os.getenv("OKX_PASSPHRASE", ""),
    },
}
