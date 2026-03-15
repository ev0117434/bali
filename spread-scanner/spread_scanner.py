"""
Spread Scanner — ищет арбитражные возможности между спотовым и фьючерсным рынком.

Стратегия: купить дешевле на споте, продать дороже на фьючерсе (или наоборот).
  Direction A: binance_spot(ask) → bybit_futures(bid)
  Direction B: bybit_spot(ask)   → binance_futures(bid)

За один цикл: один Redis pipeline с 4N HMGET (N = кол-во пар).
"""
import asyncio
import logging
import logging.handlers
import os
import random
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
MIN_SPREAD_PCT    = float(os.getenv("MIN_SPREAD_PCT", "0.1"))    # минимальный спред %
SCAN_INTERVAL_MS  = int(os.getenv("SCAN_INTERVAL_MS", "100"))    # интервал цикла мс
STALE_THRESHOLD_MS = int(os.getenv("STALE_THRESHOLD_SEC", "5")) * 1000  # устаревшие данные
SIGNAL_TTL        = int(os.getenv("SIGNAL_TTL", "60"))           # TTL сигнала в Redis (сек)
SIGNAL_CHANNEL    = "ch:spread_signals"
PROMETHEUS_PORT   = int(os.getenv("PROMETHEUS_PORT", "9091"))
ENABLE_PROMETHEUS = os.getenv("ENABLE_PROMETHEUS", "0") == "1"

# Файлы с парами символов (покупка-продажа)
_BASE = os.path.join(os.path.dirname(__file__), "..", "dictionaries", "combination")
PAIR_FILES = {
    "A": os.path.join(_BASE, "binance_spot_bybit_futures.txt"),   # binance spot → bybit futures
    "B": os.path.join(_BASE, "bybit_spot_binance_futures.txt"),   # bybit spot → binance futures
}

# Redis-ключи источника данных
_SOURCES = {
    "A": {
        "buy":  ("binance", "spot"),
        "sell": ("bybit",   "futures"),
    },
    "B": {
        "buy":  ("bybit",   "spot"),
        "sell": ("binance", "futures"),
    },
}

# Логи сигналов
LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
SIGNAL_LOG_MAX_BYTES = 50 * 1024 * 1024
SIGNAL_LOG_BACKUPS = 5


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


def make_signal_logger(logs_dir: str) -> logging.Logger:
    """Файловый логгер для сигналов с ротацией."""
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, "spread_signals.log")
    logger = logging.getLogger("spread_signals")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=SIGNAL_LOG_MAX_BYTES,
            backupCount=SIGNAL_LOG_BACKUPS,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    return logger


# ── Prometheus метрики (заглушки если не установлен) ─────────────────────────

if _PROM_AVAILABLE and ENABLE_PROMETHEUS:
    _signals_total  = Counter("spread_signals_total",  "Сигналы по направлению", ["direction"])
    _spread_gauge   = Gauge("spread_pct_current",      "Текущий спред %", ["direction", "symbol"])
    _cycles_total   = Counter("spread_cycles_total",   "Циклы сканирования")
    _stale_skipped  = Counter("spread_stale_skipped",  "Пропущено устаревших данных")
else:
    class _Noop:
        def labels(self, **_): return self
        def inc(self, *_): pass
        def set(self, *_): pass
    _signals_total = _stale_skipped = _cycles_total = _Noop()
    _spread_gauge = _Noop()


# ── Бизнес-логика ─────────────────────────────────────────────────────────────

def calc_spread(
    buy_data: list,   # [ask, ask_qty, ts_redis]  из HMGET
    sell_data: list,  # [bid, bid_qty, ts_redis]  из HMGET
    direction: str,
    symbol: str,
    ts_now: int,
    buy_key_info: tuple,   # (exchange, market)
    sell_key_info: tuple,  # (exchange, market)
) -> dict | None:
    """
    Вычисляет спред между споттовой покупкой и фьючерсной продажей.
    Возвращает сигнал-словарь или None, если спред ниже порога / данные устаревшие.

    buy_data  — результат HMGET [ask, ask_qty, ts_redis]
    sell_data — результат HMGET [bid, bid_qty, ts_redis]
    """
    buy_ask_raw, buy_qty_raw, buy_ts_raw = buy_data
    sell_bid_raw, sell_qty_raw, sell_ts_raw = sell_data

    # Проверка наличия данных
    if not buy_ask_raw or not sell_bid_raw:
        return None

    # Проверка свежести данных
    try:
        buy_ts  = int(buy_ts_raw or 0)
        sell_ts = int(sell_ts_raw or 0)
    except (ValueError, TypeError):
        return None

    if buy_ts == 0 or sell_ts == 0:
        return None

    if (ts_now - buy_ts) > STALE_THRESHOLD_MS or (ts_now - sell_ts) > STALE_THRESHOLD_MS:
        _stale_skipped.inc()
        return None

    # Вычисление спреда
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
    def __init__(self, redis_client, log, signal_logger):
        self.redis = redis_client
        self.log = log
        self.signal_logger = signal_logger
        self._symbols: dict[str, list[str]] = {}  # direction → [symbols]
        self._shutdown = asyncio.Event()
        self._reload_flag = False

    def reload_symbols(self):
        """Загрузить (или перезагрузить) символы из файлов."""
        total = 0
        for direction, filepath in PAIR_FILES.items():
            syms = load_symbols(filepath)
            self._symbols[direction] = syms
            total += len(syms)
            self.log.info("symbols_loaded", direction=direction, count=len(syms), file=filepath)
        if total == 0:
            self.log.error("no_symbols_loaded", files=list(PAIR_FILES.values()))
            sys.exit(1)

    async def _cycle(self, ts_now: int) -> int:
        """
        Один цикл сканирования. Возвращает кол-во сигналов.

        Единый Redis pipeline: 4 HMGET на каждую пару (buy ask/qty/ts + sell bid/qty/ts).
        Реально 2 HMGET на пару: один для покупки, один для продажи.
        """
        pipe = self.redis.pipeline(transaction=False)
        order: list[tuple] = []  # (direction, symbol, buy_info, sell_info)

        for direction, symbols in self._symbols.items():
            src = _SOURCES[direction]
            buy_ex,  buy_mkt  = src["buy"]
            sell_ex, sell_mkt = src["sell"]

            for sym in symbols:
                buy_key  = f"md:{buy_ex}:{buy_mkt}:{sym}"
                sell_key = f"md:{sell_ex}:{sell_mkt}:{sym}"
                pipe.hmget(buy_key,  "ask", "ask_qty", "ts_redis")
                pipe.hmget(sell_key, "bid", "bid_qty", "ts_redis")
                order.append((direction, sym, (buy_ex, buy_mkt), (sell_ex, sell_mkt)))

        results = await pipe.execute()

        signals = 0
        sig_pipe = self.redis.pipeline(transaction=False)
        sig_payloads = []

        for i, (direction, symbol, buy_info, sell_info) in enumerate(order):
            buy_data  = results[i * 2]
            sell_data = results[i * 2 + 1]

            signal = calc_spread(
                buy_data, sell_data,
                direction, symbol, ts_now,
                buy_info, sell_info,
            )
            if signal is None:
                continue

            # Записываем сигнал в Redis
            sig_key = f"sig:spread:{direction}:{symbol}"
            payload = orjson.dumps(signal).decode()
            sig_pipe.set(sig_key, payload, ex=SIGNAL_TTL)
            sig_pipe.publish(SIGNAL_CHANNEL, payload)
            sig_payloads.append((signal, payload))
            signals += 1

        if sig_payloads:
            await sig_pipe.execute()
            for signal, payload in sig_payloads:
                _signals_total.labels(direction=signal["direction"]).inc()
                self.signal_logger.info(payload)
                self.log.info(
                    "spread_signal",
                    symbol=signal["symbol"],
                    direction=signal["direction"],
                    spread_pct=signal["spread_pct"],
                    buy_ask=signal["buy_ask"],
                    sell_bid=signal["sell_bid"],
                )

        _cycles_total.inc()
        return signals

    async def run(self):
        """Основной цикл сканирования."""
        self.reload_symbols()

        if _PROM_AVAILABLE and ENABLE_PROMETHEUS:
            _prom_start(PROMETHEUS_PORT)
            self.log.info("prometheus_started", port=PROMETHEUS_PORT)

        total_symbols = sum(len(v) for v in self._symbols.values())
        self.log.info("scanner_started", total_symbols=total_symbols,
                      min_spread_pct=MIN_SPREAD_PCT, scan_interval_ms=SCAN_INTERVAL_MS)

        while not self._shutdown.is_set():
            if self._reload_flag:
                self._reload_flag = False
                self.reload_symbols()
                total_symbols = sum(len(v) for v in self._symbols.values())
                self.log.info("symbols_reloaded", total_symbols=total_symbols)

            tick_start = now_ms()
            try:
                signals = await self._cycle(tick_start)
                if signals:
                    pass  # already logged per-signal
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
    redis_client = get_redis()
    signal_logger = make_signal_logger(LOGS_DIR)
    scanner = SpreadScanner(redis_client, log, signal_logger)

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
