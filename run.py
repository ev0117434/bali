#!/usr/bin/env python3
"""
Market Data Collector — единая точка запуска.

Запускает все 6 скриптов как дочерние процессы, пишет их stdout/stderr
в общий лог с префиксом имени скрипта и в отдельные файлы в logs/.
При получении SIGTERM/SIGINT корректно останавливает все дочерние процессы.

Использование:
    python3 run.py
    python3 run.py --only binance_spot bybit_spot   # запустить выборочно
    python3 run.py --no-monitors                    # без stale/latency мониторов
    python3 run.py --logs-dir /var/log/market-data  # своя папка для логов
"""
import argparse
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
import logging

MARKET_DATA_DIR    = os.path.join(os.path.dirname(__file__), "market-data")
SPREAD_SCANNER_DIR = os.path.join(os.path.dirname(__file__), "spread-scanner")
LOGS_DIR           = os.path.join(os.path.dirname(__file__), "logs")
SIGNALS_DIR        = os.path.join(os.path.dirname(__file__), "signals")

CLEANUP_DELAY = int(os.getenv("CLEANUP_DELAY", "3"))  # секунды обратного отсчёта

# Директория скрипта (по умолчанию MARKET_DATA_DIR, кроме исключений)
SCRIPT_DIRS = {
    "spread_scanner":    SPREAD_SCANNER_DIR,
    "signal_snapshot":   SPREAD_SCANNER_DIR,
}

# Ротация: 50 МБ на файл, 5 архивов → макс 300 МБ на скрипт
LOG_MAX_BYTES = 50 * 1024 * 1024
LOG_BACKUP_COUNT = 5

SCRIPTS = [
    "binance_spot",
    "binance_futures",
    "bybit_spot",
    "bybit_futures",
    "okx_spot",
    "okx_futures",
    "gate_spot",
    "gate_futures",
    "stale_monitor",
    "latency_monitor",
    "spread_scanner",
    "signal_snapshot",
]

MONITORS = {"stale_monitor", "latency_monitor"}

# ANSI colours (отключаются если не TTY)
_USE_COLOR = sys.stdout.isatty()
COLORS = {
    "binance_spot":    "\033[94m",  # синий
    "binance_futures": "\033[96m",  # голубой
    "bybit_spot":      "\033[92m",  # зелёный
    "bybit_futures":   "\033[93m",  # жёлтый
    "okx_spot":        "\033[97m",  # белый
    "okx_futures":     "\033[37m",  # светло-серый
    "gate_spot":       "\033[32m",  # тёмно-зелёный
    "gate_futures":    "\033[36m",  # циан
    "stale_monitor":   "\033[95m",  # фиолетовый
    "latency_monitor": "\033[91m",  # красный
    "spread_scanner":  "\033[33m",  # оранжевый
    "signal_snapshot": "\033[35m",  # пурпурный
}
RESET = "\033[0m"


def startup_cleanup(logs_dir: str) -> None:
    """
    Выполняется при старте:
      1. Удаляет папку логов (logs/)
      2. Удаляет папку сигналов (signals/)
      3. Очищает текущую БД Redis (FLUSHDB)
    Перед удалением показывает countdown CLEANUP_DELAY секунд.
    """
    print(f"[run.py] Очистка данных через {CLEANUP_DELAY} сек... (Ctrl+C для отмены)", flush=True)
    try:
        for i in range(CLEANUP_DELAY, 0, -1):
            print(f"[run.py]   {i}...", flush=True)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[run.py] Очистка отменена.", flush=True)
        sys.exit(0)

    # 1. Папка логов
    if os.path.exists(logs_dir):
        shutil.rmtree(logs_dir, ignore_errors=True)
        print(f"[run.py] Удалена папка логов:    {logs_dir}", flush=True)

    # 2. Папка сигналов
    if os.path.exists(SIGNALS_DIR):
        shutil.rmtree(SIGNALS_DIR, ignore_errors=True)
        print(f"[run.py] Удалена папка сигналов: {SIGNALS_DIR}", flush=True)

    # 3. Redis FLUSHDB
    try:
        try:
            from dotenv import load_dotenv
            _env_file = os.path.join(MARKET_DATA_DIR, ".env")
            if os.path.exists(_env_file):
                load_dotenv(_env_file)
        except ImportError:
            pass
        import redis as _redis_sync
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        _r = _redis_sync.Redis.from_url(redis_url)
        _r.flushdb()
        _r.close()
        print(f"[run.py] Redis очищен (FLUSHDB): {redis_url}", flush=True)
    except Exception as exc:
        print(f"[run.py] Не удалось очистить Redis: {exc}", flush=True)

    print("-" * 60, flush=True)


def colorize(name: str, text: str) -> str:
    if not _USE_COLOR:
        return text
    return f"{COLORS.get(name, '')}{text}{RESET}"


def make_file_logger(name: str, logs_dir: str) -> logging.Logger:
    """Создаёт ротируемый файловый логгер для скрипта."""
    log_path = os.path.join(logs_dir, f"{name}.log")
    logger = logging.getLogger(f"file.{name}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    handler = RotatingFileHandler(
        log_path,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


def stream_output(proc: subprocess.Popen, name: str, stop_event: threading.Event,
                  file_logger: logging.Logger):
    """Читает stdout процесса, пишет в терминал с префиксом и в лог-файл."""
    prefix = colorize(name, f"[{name}]")
    try:
        for line in iter(proc.stdout.readline, b""):
            if stop_event.is_set():
                break
            text = line.decode("utf-8", errors="replace").rstrip()
            print(f"{prefix} {text}", flush=True)
            file_logger.info(text)
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description="Market Data Collector launcher")
    parser.add_argument(
        "--only", nargs="+", metavar="SCRIPT",
        help=f"Запустить только указанные скрипты. Доступные: {', '.join(SCRIPTS)}",
    )
    parser.add_argument(
        "--no-monitors", action="store_true",
        help="Не запускать stale_monitor и latency_monitor"
    )
    parser.add_argument(
        "--logs-dir", metavar="DIR", default=LOGS_DIR,
        help=f"Папка для лог-файлов (по умолчанию: logs/)"
    )
    parser.add_argument(
        "--no-cleanup", action="store_true",
        help="Не очищать Redis и не удалять logs/ и signals/ при старте"
    )
    args = parser.parse_args()

    # Определяем список скриптов для запуска
    if args.only:
        unknown = set(args.only) - set(SCRIPTS)
        if unknown:
            print(f"Неизвестные скрипты: {unknown}. Доступные: {SCRIPTS}", file=sys.stderr)
            sys.exit(1)
        to_run = [s for s in SCRIPTS if s in args.only]
    elif args.no_monitors:
        to_run = [s for s in SCRIPTS if s not in MONITORS]
    else:
        to_run = SCRIPTS

    logs_dir = os.path.abspath(args.logs_dir)

    # Очистка Redis, logs/ и signals/ при старте (если не отключена)
    if not args.no_cleanup:
        startup_cleanup(logs_dir)

    # Создаём папку для логов (могла быть удалена cleanup-ом)
    os.makedirs(logs_dir, exist_ok=True)

    # Лог самого run.py
    run_logger = make_file_logger("run", logs_dir)

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Запуск {len(to_run)} скриптов: {', '.join(to_run)}", flush=True)
    print(f"Рабочая директория: {MARKET_DATA_DIR}", flush=True)
    print(f"Логи: {logs_dir}/", flush=True)
    print(f"Для остановки: Ctrl+C или SIGTERM", flush=True)
    print("-" * 60, flush=True)
    run_logger.info(f"[{started_at}] Запуск: {', '.join(to_run)}")

    processes: dict[str, subprocess.Popen] = {}
    threads: list[threading.Thread] = []
    stop_event = threading.Event()

    # Запускаем процессы
    for name in to_run:
        script_dir = SCRIPT_DIRS.get(name, MARKET_DATA_DIR)
        script_path = os.path.join(script_dir, f"{name}.py")
        if not os.path.exists(script_path):
            print(f"[WARN] {script_path} не найден, пропускаем", file=sys.stderr)
            continue

        env = os.environ.copy()
        # Оба каталога в PYTHONPATH — spread_scanner импортирует common из market-data
        env["PYTHONPATH"] = (
            script_dir + os.pathsep + MARKET_DATA_DIR + os.pathsep + env.get("PYTHONPATH", "")
        )

        proc = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # stderr → в тот же поток
            cwd=script_dir,
            env=env,
        )
        processes[name] = proc

        file_logger = make_file_logger(name, logs_dir)
        t = threading.Thread(
            target=stream_output,
            args=(proc, name, stop_event, file_logger),
            daemon=True,
        )
        t.start()
        threads.append(t)

        msg = f"Запущен {name} (PID {proc.pid}) → {logs_dir}/{name}.log"
        print(f"  {msg}", flush=True)
        run_logger.info(msg)
        time.sleep(0.1)  # небольшая задержка между стартами

    if not processes:
        print("Нет скриптов для запуска.", file=sys.stderr)
        sys.exit(1)

    print("-" * 60, flush=True)

    shutdown_requested = threading.Event()

    def handle_signal(signum, frame):
        if not shutdown_requested.is_set():
            shutdown_requested.set()
            print("\n[run.py] Получен сигнал остановки, завершаем процессы...", flush=True)
            stop_event.set()
            for name, proc in processes.items():
                if proc.poll() is None:
                    print(f"[run.py] SIGTERM → {name} (PID {proc.pid})", flush=True)
                    proc.send_signal(signal.SIGTERM)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Мониторим процессы: если один упал — логируем, не перезапускаем
    # (перезапуск — задача supervisord/systemd)
    try:
        while not shutdown_requested.is_set():
            for name, proc in list(processes.items()):
                rc = proc.poll()
                if rc is not None:
                    prefix = colorize(name, f"[{name}]")
                    msg = f"процесс завершился с кодом {rc}"
                    print(f"{prefix} {msg}", flush=True)
                    run_logger.info(f"[{name}] {msg}")
                    del processes[name]

            if not processes:
                print("[run.py] Все процессы завершились.", flush=True)
                break

            time.sleep(1)
    except KeyboardInterrupt:
        handle_signal(signal.SIGINT, None)

    # Ждём завершения (grace period 10 сек)
    grace = 10
    deadline = time.time() + grace
    for name, proc in processes.items():
        remaining = max(0, deadline - time.time())
        try:
            proc.wait(timeout=remaining)
            print(f"[run.py] {name} завершён корректно.", flush=True)
            run_logger.info(f"[{name}] завершён корректно")
        except subprocess.TimeoutExpired:
            print(f"[run.py] {name} не ответил за {grace}с — SIGKILL", flush=True)
            run_logger.warning(f"[{name}] SIGKILL после {grace}с")
            proc.kill()

    stop_event.set()
    for t in threads:
        t.join(timeout=2)

    print("[run.py] Готово.", flush=True)
    run_logger.info("Завершено")


if __name__ == "__main__":
    main()
