"""
Spread Scanner — непрерывный поиск арбитражного спреда между спотом и фьючерсом.

Цикл (каждые SCAN_INTERVAL_MS мс):
  1. Один Redis pipeline: 2N HMGET для всех пар
  2. calc_spread() — чистая функция, без I/O
  3. Если спред >= MIN_SPREAD_PCT И нет cooldown → пишем в signals/
  4. Запись в logs/spread_scanner.log о каждом цикле

Cooldown: найденный сигнал по (direction, symbol) не пишется в signals/
повторно в течение COOLDOWN_SEC. Сканирование продолжается всегда.

Directions:
  A: binance_spot(ask) → bybit_futures(bid)
  B: bybit_spot(ask)   → binance_futures(bid)
  C: okx_spot(ask)     → binance_futures(bid)
  D: binance_spot(ask) → okx_futures(bid)
  E: okx_spot(ask)     → bybit_futures(bid)
  F: bybit_spot(ask)   → okx_futures(bid)
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
MIN_SPREAD_PCT    = float(os.getenv("MIN_SPREAD_PCT",    "1.0"))
SCAN_INTERVAL_MS  = int(os.getenv("SCAN_INTERVAL_MS",   "200"))   # 0.2 сек
STALE_THRESHOLD_MS = int(os.getenv("STALE_THRESHOLD_SEC", "5")) * 1000
SIGNAL_TTL        = int(os.getenv("SIGNAL_TTL",         "60"))    # Redis key TTL
COOLDOWN_SEC      = int(os.getenv("SIGNAL_COOLDOWN_SEC", "3600")) # 1 час
COOLDOWN_MS       = COOLDOWN_SEC * 1000
SIGNAL_CHANNEL    = "ch:spread_signals"
PROMETHEUS_PORT   = int(os.getenv("PROMETHEUS_PORT",    "9091"))
ENABLE_PROMETHEUS = os.getenv("ENABLE_PROMETHEUS", "0") == "1"

# Файлы с парами символов
_BASE = os.path.join(os.path.dirname(__file__), "..", "dictionaries", "combination")
PAIR_FILES = {
    "A": os.path.join(_BASE, "binance_spot_bybit_futures.txt"),
    "B": os.path.join(_BASE, "bybit_spot_binance_futures.txt"),
    "C": os.path.join(_BASE, "okx_spot_binance_futures.txt"),
    "D": os.path.join(_BASE, "binance_spot_okx_futures.txt"),
    "E": os.path.join(_BASE, "okx_spot_bybit_futures.txt"),
    "F": os.path.join(_BASE, "bybit_spot_okx_futures.txt"),
}

_SOURCES = {
    "A": {"buy": ("binance", "spot"),  "sell": ("bybit",   "futures")},
    "B": {"buy": ("bybit",   "spot"),  "sell": ("binance", "futures")},
    "C": {"buy": ("okx",     "spot"),  "sell": ("binance", "futures")},
    "D": {"buy": ("binance", "spot"),  "sell": ("okx",     "futures")},
    "E": {"buy": ("okx",     "spot"),  "sell": ("bybit",   "futures")},
    "F": {"buy": ("bybit",   "spot"),  "sell": ("okx",     "futures")},
}

# Папки
_ROOT       = os.path.join(os.path.dirname(__file__), "..")
LOGS_DIR    = os.path.join(_ROOT, "logs")
SIGNALS_DIR = os.path.join(_ROOT, "signals")

# Параметры ротации
_50MB  = 50  * 1024 * 1024
_100MB = 100 * 1024 * 1024


# ── Логгеры ───────────────────────────────────────────────────────────────────

def _rotating(name: str, path: str, max_bytes: int, backups: int) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        h = logging.handlers.RotatingFileHandler(
            path, maxBytes=max_bytes, backupCount=backups, encoding="utf-8"
        )
        h.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(h)
    return logger


def make_activity_logger() -> logging.Logger:
    """logs/spread_scanner.log — каждый цикл сканирования."""
    os.makedirs(LOGS_DIR, exist_ok=True)
    return _rotating("ss.activity", os.path.join(LOGS_DIR, "spread_scanner.log"), _50MB, 5)


def make_signal_logger() -> logging.Logger:
    """signals/spread_signals.jsonl — один JSON на каждый новый сигнал."""
    os.makedirs(SIGNALS_DIR, exist_ok=True)
    return _rotating("ss.signals", os.path.join(SIGNALS_DIR, "spread_signals.jsonl"), _100MB, 10)


# ── Prometheus заглушки ───────────────────────────────────────────────────────

if _PROM_AVAILABLE and ENABLE_PROMETHEUS:
    _sig_counter  = Counter("spread_signals_total",  "Новые сигналы", ["direction"])
    _spread_gauge = Gauge("spread_pct",              "Текущий спред %", ["direction", "symbol"])
    _cycle_count  = Counter("spread_cycles_total",   "Циклов всего")
else:
    class _Noop:
        def labels(self, **_): return self
        def inc(self, *_): pass
        def set(self, *_): pass
    _sig_counter = _spread_gauge = _cycle_count = _Noop()


# ── Бизнес-логика ─────────────────────────────────────────────────────────────

def load_symbols(filepath: str) -> list[str]:
    if not os.path.exists(filepath):
        return []
    out = []
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                out.append(line.upper())
    return list(dict.fromkeys(out))


def calc_spread(
    buy_data: list,        # [ask, ask_qty, ts_redis]
    sell_data: list,       # [bid, bid_qty, ts_redis]
    direction: str,
    symbol: str,
    ts_now: int,
    buy_info: tuple,       # (exchange, market)
    sell_info: tuple,      # (exchange, market)
) -> dict | None:
    """
    Чистая функция. Возвращает сигнал-словарь если spread_pct >= MIN_SPREAD_PCT,
    иначе None. Данные старше STALE_THRESHOLD_MS игнорируются.
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
        "buy_exchange":  buy_info[0],
        "buy_market":    buy_info[1],
        "buy_ask":       buy_ask_raw,
        "buy_ask_qty":   buy_qty_raw or "",
        "sell_exchange": sell_info[0],
        "sell_market":   sell_info[1],
        "sell_bid":      sell_bid_raw,
        "sell_bid_qty":  sell_qty_raw or "",
        "spread_pct":    round(spread_pct, 4),
        "ts_signal":     ts_now,
    }


# ── Сканер ────────────────────────────────────────────────────────────────────

class SpreadScanner:
    def __init__(self, redis_client, log):
        self.redis          = redis_client
        self.log            = log                 # structlog → stdout
        self.activity_log   = make_activity_logger()  # logs/spread_scanner.log
        self.signal_log     = make_signal_logger()    # signals/spread_signals.jsonl
        self._symbols: dict[str, list[str]] = {}
        self._shutdown    = asyncio.Event()
        self._reload_flag = False
        # (direction, symbol) → ts_ms когда последний раз записан сигнал в signals/
        self._cooldown: dict[tuple[str, str], int] = {}
        self._n           = 0   # счётчик циклов

    # ── Символы ──────────────────────────────────────────────────────────────

    def reload_symbols(self):
        total = 0
        for direction, filepath in PAIR_FILES.items():
            syms = load_symbols(filepath)
            self._symbols[direction] = syms
            total += len(syms)
            self.log.info("symbols_loaded", direction=direction, count=len(syms))
        if total == 0:
            self.log.error("no_symbols_loaded")
            sys.exit(1)

    def _cleanup_cooldown(self, ts_now: int):
        expired = [k for k, v in self._cooldown.items() if ts_now - v >= COOLDOWN_MS]
        for k in expired:
            del self._cooldown[k]

    # ── Основной цикл ─────────────────────────────────────────────────────────

    async def _cycle(self, ts_now: int) -> tuple[int, int, int]:
        """
        Один цикл. Возвращает (pairs_scanned, signals_written, suppressed).
        Сканирование выполняется всегда. В signals/ пишем только если нет cooldown.
        """
        # ── 1. Собираем pipeline ─────────────────────────────────────────────
        pipe = self.redis.pipeline(transaction=False)
        order: list[tuple] = []

        for direction, symbols in self._symbols.items():
            src = _SOURCES[direction]
            bex, bmkt = src["buy"]
            sex, smkt = src["sell"]
            for sym in symbols:
                pipe.hmget(f"md:{bex}:{bmkt}:{sym}",  "ask", "ask_qty", "ts_redis")
                pipe.hmget(f"md:{sex}:{smkt}:{sym}",  "bid", "bid_qty", "ts_redis")
                order.append((direction, sym, (bex, bmkt), (sex, smkt)))

        results = await pipe.execute()

        # ── 2. Вычисляем спреды ───────────────────────────────────────────────
        to_write: list[tuple[dict, str]] = []   # (signal, json)
        suppressed = 0

        for i, (direction, symbol, buy_info, sell_info) in enumerate(order):
            sig = calc_spread(
                results[i * 2], results[i * 2 + 1],
                direction, symbol, ts_now, buy_info, sell_info,
            )
            if sig is None:
                continue

            # Cooldown: только блокирует запись в signals/, сканирование идёт всегда
            key = (direction, symbol)
            last = self._cooldown.get(key, 0)
            if ts_now - last < COOLDOWN_MS:
                suppressed += 1
                continue

            payload = orjson.dumps(sig).decode()
            to_write.append((sig, payload))
            self._cooldown[key] = ts_now

        # ── 3. Пишем сигналы (Redis + signals/) ──────────────────────────────
        if to_write:
            sig_pipe = self.redis.pipeline(transaction=False)
            for sig, payload in to_write:
                sig_pipe.set(f"sig:spread:{sig['direction']}:{sig['symbol']}", payload, ex=SIGNAL_TTL)
                sig_pipe.publish(SIGNAL_CHANNEL, payload)
            await sig_pipe.execute()

            for sig, payload in to_write:
                _sig_counter.labels(direction=sig["direction"]).inc()
                self.signal_log.info(payload)    # → signals/spread_signals.jsonl
                self.log.info(                   # → stdout
                    "spread_signal",
                    symbol=sig["symbol"],
                    direction=sig["direction"],
                    spread_pct=sig["spread_pct"],
                    buy_ask=sig["buy_ask"],
                    sell_bid=sig["sell_bid"],
                )

        _cycle_count.inc()

        # Очистка устаревших cooldown-записей каждые 1000 циклов (~3 мин)
        self._n += 1
        if self._n % 1000 == 0:
            self._cleanup_cooldown(ts_now)

        return len(order), len(to_write), suppressed

    # ── Запуск ────────────────────────────────────────────────────────────────

    async def run(self):
        self.reload_symbols()

        if _PROM_AVAILABLE and ENABLE_PROMETHEUS:
            _prom_start(PROMETHEUS_PORT)
            self.log.info("prometheus_started", port=PROMETHEUS_PORT)

        total_pairs = sum(len(v) for v in self._symbols.values())
        start_msg = orjson.dumps({
            "event":        "scanner_started",
            "total_pairs":  total_pairs,
            "min_spread_pct": MIN_SPREAD_PCT,
            "scan_interval_ms": SCAN_INTERVAL_MS,
            "cooldown_sec": COOLDOWN_SEC,
        }).decode()
        self.log.info("scanner_started", total_pairs=total_pairs,
                      min_spread_pct=MIN_SPREAD_PCT, scan_interval_ms=SCAN_INTERVAL_MS,
                      cooldown_sec=COOLDOWN_SEC)
        self.activity_log.info(start_msg)

        while not self._shutdown.is_set():
            if self._reload_flag:
                self._reload_flag = False
                self.reload_symbols()
                total_pairs = sum(len(v) for v in self._symbols.values())
                self.log.info("symbols_reloaded", total_pairs=total_pairs)

            tick_start = now_ms()
            pairs = written = suppressed = 0
            error = None

            try:
                pairs, written, suppressed = await self._cycle(tick_start)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error = str(exc)
                self.log.error("cycle_error", error=error, exc_info=True)

            elapsed = now_ms() - tick_start

            # Запись в logs/spread_scanner.log о каждом цикле
            entry = {
                "ts":           tick_start,
                "cycle":        self._n,
                "pairs":        pairs,
                "signals":      written,
                "suppressed":   suppressed,
                "elapsed_ms":   elapsed,
            }
            if error:
                entry["error"] = error
            self.activity_log.info(orjson.dumps(entry).decode())

            sleep_ms = max(0, SCAN_INTERVAL_MS - elapsed)
            if sleep_ms > 0:
                await asyncio.sleep(sleep_ms / 1000)

        stop_msg = orjson.dumps({"event": "scanner_stopped", "cycles": self._n}).decode()
        self.log.info("scanner_stopped", cycles=self._n)
        self.activity_log.info(stop_msg)


# ── Точка входа ───────────────────────────────────────────────────────────────

async def main():
    log = setup_logging("spread_scanner")
    redis_client = get_redis()
    scanner = SpreadScanner(redis_client, log)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, scanner._shutdown.set)

    def _sighup():
        scanner._reload_flag = True
        log.info("sighup_received")

    loop.add_signal_handler(signal.SIGHUP, _sighup)

    try:
        await scanner.run()
    finally:
        await redis_client.aclose()
        log.info("graceful_shutdown")


if __name__ == "__main__":
    asyncio.run(main())
