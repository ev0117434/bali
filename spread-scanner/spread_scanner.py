"""
Spread Scanner — ищет арбитражные возможности между спотовым и фьючерсным рынком.

Стратегия: купить дешевле на споте, продать дороже на фьючерсе (или наоборот).
  Direction A: binance_spot(ask) → bybit_futures(bid)
  Direction B: bybit_spot(ask)   → binance_futures(bid)

За один цикл: один Redis pipeline с 2N HMGET (N = кол-во пар).

Дедупликация: сигнал по одному (direction, symbol) не повторяется в течение COOLDOWN_MS.
Снапшоты: каждый сигнал пишется в signals/spread_signals.jsonl (проект) и logs/spread_signals.log.
"""
import asyncio
import logging
import logging.handlers
import os
import signal
import sys
import time

import orjson

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "market-data"))
from common import get_redis, now_ms, setup_logging

# ── Prometheus (опционально) ──────────────────────────────────────────────────
try:
    from prometheus_client import Counter, Gauge, start_http_server as _prom_start
    _PROM_AVAILABLE = True
except ImportError:
    _PROM_AVAILABLE = False

# ── Конфигурация ──────────────────────────────────────────────────────────────
MIN_SPREAD_PCT     = float(os.getenv("MIN_SPREAD_PCT", "1.0"))      # мин. спред %
SCAN_INTERVAL_MS   = int(os.getenv("SCAN_INTERVAL_MS", "100"))      # интервал цикла мс
STALE_THRESHOLD_MS = int(os.getenv("STALE_THRESHOLD_SEC", "5")) * 1000  # устаревшие данные
SIGNAL_TTL         = int(os.getenv("SIGNAL_TTL", "60"))             # TTL ключа сигнала в Redis (сек)
COOLDOWN_MS        = int(os.getenv("SIGNAL_COOLDOWN_SEC", "3600")) * 1000  # повтор не раньше 1 ч
SIGNAL_CHANNEL     = "ch:spread_signals"
PROMETHEUS_PORT    = int(os.getenv("PROMETHEUS_PORT", "9091"))
ENABLE_PROMETHEUS  = os.getenv("ENABLE_PROMETHEUS", "0") == "1"

# Файлы с парами символов (покупка-продажа)
_BASE = os.path.join(os.path.dirname(__file__), "..", "dictionaries", "combination")
PAIR_FILES = {
    "A": os.path.join(_BASE, "binance_spot_bybit_futures.txt"),   # binance spot → bybit futures
    "B": os.path.join(_BASE, "bybit_spot_binance_futures.txt"),   # bybit spot → binance futures
}

# Redis-ключи источника данных
_SOURCES = {
    "A": {"buy": ("binance", "spot"),  "sell": ("bybit",   "futures")},
    "B": {"buy": ("bybit",   "spot"),  "sell": ("binance", "futures")},
}

# Папки для логов и снапшотов
LOGS_DIR    = os.path.join(os.path.dirname(__file__), "..", "logs")
SIGNALS_DIR = os.path.join(os.path.dirname(__file__), "..", "signals")

LOG_MAX_BYTES   = 50 * 1024 * 1024
LOG_BACKUP_COUNT = 5
SNAPSHOT_MAX_BYTES   = 100 * 1024 * 1024   # 100 МБ на файл снапшотов
SNAPSHOT_BACKUP_COUNT = 10


# ── Утилиты ───────────────────────────────────────────────────────────────────

def load_symbols(filepath: str) -> list[str]:
    """Загружает символы из файла. Пропускает пустые строки и #-комментарии."""
    if not os.path.exists(filepath):
        return []
    symbols = []
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                symbols.append(line.upper())
    return list(dict.fromkeys(symbols))  # dedup, preserve order


def _make_file_logger(name: str, path: str, max_bytes: int, backup_count: int) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.handlers.RotatingFileHandler(
            path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    return logger


def make_signal_logger(logs_dir: str) -> logging.Logger:
    """Лог сигналов в logs/spread_signals.log (ротируемый)."""
    os.makedirs(logs_dir, exist_ok=True)
    return _make_file_logger(
        "spread_signals",
        os.path.join(logs_dir, "spread_signals.log"),
        LOG_MAX_BYTES, LOG_BACKUP_COUNT,
    )


def make_snapshot_logger(signals_dir: str) -> logging.Logger:
    """Снапшоты сигналов в signals/spread_signals.jsonl (100 МБ × 10 архивов)."""
    os.makedirs(signals_dir, exist_ok=True)
    return _make_file_logger(
        "spread_snapshots",
        os.path.join(signals_dir, "spread_signals.jsonl"),
        SNAPSHOT_MAX_BYTES, SNAPSHOT_BACKUP_COUNT,
    )


# ── Prometheus метрики (заглушки если не установлен) ─────────────────────────

if _PROM_AVAILABLE and ENABLE_PROMETHEUS:
    _signals_total = Counter("spread_signals_total",  "Сигналы по направлению", ["direction"])
    _spread_gauge  = Gauge("spread_pct_current",      "Текущий спред %", ["direction", "symbol"])
    _cycles_total  = Counter("spread_cycles_total",   "Циклы сканирования")
    _stale_skipped = Counter("spread_stale_skipped",  "Пропущено устаревших данных")
else:
    class _Noop:
        def labels(self, **_): return self
        def inc(self, *_): pass
        def set(self, *_): pass
    _signals_total = _stale_skipped = _cycles_total = _Noop()
    _spread_gauge = _Noop()


# ── Бизнес-логика ─────────────────────────────────────────────────────────────

def calc_spread(
    buy_data: list,        # [ask, ask_qty, ts_redis]  из HMGET
    sell_data: list,       # [bid, bid_qty, ts_redis]  из HMGET
    direction: str,
    symbol: str,
    ts_now: int,
    buy_key_info: tuple,   # (exchange, market)
    sell_key_info: tuple,  # (exchange, market)
) -> dict | None:
    """
    Вычисляет спред. Возвращает сигнал-словарь если spread_pct >= MIN_SPREAD_PCT, иначе None.
    Проверяет свежесть данных (оба конца должны быть не старше STALE_THRESHOLD_MS).
    """
    buy_ask_raw, buy_qty_raw, buy_ts_raw = buy_data
    sell_bid_raw, sell_qty_raw, sell_ts_raw = sell_data

    if not buy_ask_raw or not sell_bid_raw:
        return None

    try:
        buy_ts  = int(buy_ts_raw  or 0)
        sell_ts = int(sell_ts_raw or 0)
    except (ValueError, TypeError):
        return None

    if buy_ts == 0 or sell_ts == 0:
        return None

    if (ts_now - buy_ts) > STALE_THRESHOLD_MS or (ts_now - sell_ts) > STALE_THRESHOLD_MS:
        _stale_skipped.inc()
        return None

    try:
        buy_ask  = float(buy_ask_raw)
        sell_bid = float(sell_bid_raw)
    except (ValueError, TypeError):
        return None

    if buy_ask <= 0:
        return None

    spread_pct = (sell_bid - buy_ask) / buy_ask * 100

    _spread_gauge.labels(direction=direction, symbol=symbol).set(spread_pct)

    if spread_pct < MIN_SPREAD_PCT:
        return None

    return {
        "symbol":        symbol,
        "direction":     direction,
        "buy_exchange":  buy_key_info[0],
        "buy_market":    buy_key_info[1],
        "buy_ask":       buy_ask_raw,
        "buy_ask_qty":   buy_qty_raw or "",
        "sell_exchange": sell_key_info[0],
        "sell_market":   sell_key_info[1],
        "sell_bid":      sell_bid_raw,
        "sell_bid_qty":  sell_qty_raw or "",
        "spread_pct":    round(spread_pct, 4),
        "ts_signal":     ts_now,
    }


# ── Сканер ────────────────────────────────────────────────────────────────────

class SpreadScanner:
    def __init__(self, redis_client, log, signal_logger, snapshot_logger):
        self.redis           = redis_client
        self.log             = log
        self.signal_logger   = signal_logger    # logs/spread_signals.log
        self.snapshot_logger = snapshot_logger  # signals/spread_signals.jsonl
        self._symbols: dict[str, list[str]] = {}
        self._shutdown    = asyncio.Event()
        self._reload_flag = False
        # key: (direction, symbol) → ts_ms когда последний раз отправлен сигнал
        self._cooldown: dict[tuple[str, str], int] = {}
        self._cycle_count = 0

    def reload_symbols(self):
        total = 0
        for direction, filepath in PAIR_FILES.items():
            syms = load_symbols(filepath)
            self._symbols[direction] = syms
            total += len(syms)
            self.log.info("symbols_loaded", direction=direction, count=len(syms), file=filepath)
        if total == 0:
            self.log.error("no_symbols_loaded", files=list(PAIR_FILES.values()))
            sys.exit(1)

    def _cleanup_cooldown(self, ts_now: int):
        """Удаляет просроченные записи из cooldown-словаря."""
        expired = [k for k, v in self._cooldown.items() if ts_now - v >= COOLDOWN_MS]
        for k in expired:
            del self._cooldown[k]

    async def _cycle(self, ts_now: int) -> tuple[int, int]:
        """
        Один цикл сканирования.
        Возвращает (signals_emitted, signals_suppressed_by_cooldown).
        """
        pipe = self.redis.pipeline(transaction=False)
        order: list[tuple] = []  # (direction, symbol, buy_info, sell_info)

        for direction, symbols in self._symbols.items():
            src = _SOURCES[direction]
            buy_ex,  buy_mkt  = src["buy"]
            sell_ex, sell_mkt = src["sell"]
            for sym in symbols:
                pipe.hmget(f"md:{buy_ex}:{buy_mkt}:{sym}",  "ask", "ask_qty", "ts_redis")
                pipe.hmget(f"md:{sell_ex}:{sell_mkt}:{sym}", "bid", "bid_qty", "ts_redis")
                order.append((direction, sym, (buy_ex, buy_mkt), (sell_ex, sell_mkt)))

        results = await pipe.execute()

        emitted   = 0
        suppressed = 0
        sig_pipe   = self.redis.pipeline(transaction=False)
        to_emit: list[tuple[dict, str]] = []  # (signal_dict, json_payload)

        for i, (direction, symbol, buy_info, sell_info) in enumerate(order):
            sig = calc_spread(
                results[i * 2], results[i * 2 + 1],
                direction, symbol, ts_now,
                buy_info, sell_info,
            )
            if sig is None:
                continue

            # Проверка cooldown: не чаще чем раз в COOLDOWN_MS
            cooldown_key = (direction, symbol)
            last_ts = self._cooldown.get(cooldown_key, 0)
            if ts_now - last_ts < COOLDOWN_MS:
                suppressed += 1
                continue

            payload = orjson.dumps(sig).decode()
            sig_pipe.set(f"sig:spread:{direction}:{symbol}", payload, ex=SIGNAL_TTL)
            sig_pipe.publish(SIGNAL_CHANNEL, payload)
            to_emit.append((sig, payload))
            self._cooldown[cooldown_key] = ts_now

        if to_emit:
            await sig_pipe.execute()
            for sig, payload in to_emit:
                _signals_total.labels(direction=sig["direction"]).inc()
                self.signal_logger.info(payload)
                self.snapshot_logger.info(payload)
                self.log.info(
                    "spread_signal",
                    symbol=sig["symbol"],
                    direction=sig["direction"],
                    spread_pct=sig["spread_pct"],
                    buy_ask=sig["buy_ask"],
                    sell_bid=sig["sell_bid"],
                )
            emitted = len(to_emit)

        _cycles_total.inc()

        # Очистка просроченных cooldown-записей каждые 1000 циклов
        self._cycle_count += 1
        if self._cycle_count % 1000 == 0:
            self._cleanup_cooldown(ts_now)

        return emitted, suppressed

    async def run(self):
        self.reload_symbols()

        if _PROM_AVAILABLE and ENABLE_PROMETHEUS:
            _prom_start(PROMETHEUS_PORT)
            self.log.info("prometheus_started", port=PROMETHEUS_PORT)

        total_symbols = sum(len(v) for v in self._symbols.values())
        self.log.info(
            "scanner_started",
            total_symbols=total_symbols,
            min_spread_pct=MIN_SPREAD_PCT,
            scan_interval_ms=SCAN_INTERVAL_MS,
            cooldown_sec=COOLDOWN_MS // 1000,
        )

        while not self._shutdown.is_set():
            if self._reload_flag:
                self._reload_flag = False
                self.reload_symbols()
                total_symbols = sum(len(v) for v in self._symbols.values())
                self.log.info("symbols_reloaded", total_symbols=total_symbols)

            tick_start = now_ms()
            try:
                await self._cycle(tick_start)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.log.error("cycle_error", error=str(exc), exc_info=True)

            elapsed = now_ms() - tick_start
            sleep_ms = max(0, SCAN_INTERVAL_MS - elapsed)
            if sleep_ms > 0:
                await asyncio.sleep(sleep_ms / 1000)

        self.log.info("scanner_stopped")


# ── Точка входа ───────────────────────────────────────────────────────────────

async def main():
    log = setup_logging("spread_scanner")
    redis_client   = get_redis()
    signal_logger  = make_signal_logger(LOGS_DIR)
    snapshot_logger = make_snapshot_logger(SIGNALS_DIR)
    scanner = SpreadScanner(redis_client, log, signal_logger, snapshot_logger)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, scanner._shutdown.set)

    def _sighup_handler():
        scanner._reload_flag = True
        log.info("sighup_received")

    loop.add_signal_handler(signal.SIGHUP, _sighup_handler)

    try:
        await scanner.run()
    finally:
        await redis_client.aclose()
        log.info("graceful_shutdown")


if __name__ == "__main__":
    asyncio.run(main())
