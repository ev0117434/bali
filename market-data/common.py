import os
import sys
import time
import logging

import orjson
import structlog
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Constants (read once at import time)
# ---------------------------------------------------------------------------
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_KEY_TTL: int = int(os.getenv("REDIS_KEY_TTL", "300"))
SYMBOLS_PER_CONN: int = int(os.getenv("SYMBOLS_PER_CONN", "150"))
WS_RECV_TIMEOUT: int = int(os.getenv("WS_RECV_TIMEOUT", "60"))
RECONNECT_MAX_DELAY: int = int(os.getenv("RECONNECT_MAX_DELAY", "60"))
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logging(service_name: str) -> structlog.BoundLogger:
    level = getattr(logging, LOG_LEVEL, logging.INFO)
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=lambda v, **kw: orjson.dumps(v).decode()),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )
    return structlog.get_logger().bind(service=service_name)


# ---------------------------------------------------------------------------
# Symbol loading
# ---------------------------------------------------------------------------
def load_symbols(filepath: str) -> list[str]:
    log = structlog.get_logger()
    if not os.path.exists(filepath):
        log.critical("symbols_file_not_found", filepath=filepath)
        sys.exit(1)

    seen: set[str] = set()
    result: list[str] = []
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            s = s.upper()
            if s not in seen:
                seen.add(s)
                result.append(s)

    if not result:
        log.critical("symbols_file_empty", filepath=filepath)
        sys.exit(1)

    return result


# ---------------------------------------------------------------------------
# Redis client
# ---------------------------------------------------------------------------
def get_redis() -> aioredis.Redis:
    return aioredis.Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        retry_on_error=[aioredis.ConnectionError, aioredis.TimeoutError],
    )


# ---------------------------------------------------------------------------
# Time
# ---------------------------------------------------------------------------
def now_ms() -> int:
    return int(time.time() * 1000)
