#!/usr/bin/env python3
"""
Market Data Collector — единая точка запуска.

Запускает все 6 скриптов как дочерние процессы, пишет их stdout/stderr
в общий лог с префиксом имени скрипта. При получении SIGTERM/SIGINT
корректно останавливает все дочерние процессы.

Использование:
    python3 run.py
    python3 run.py --only binance_spot bybit_spot   # запустить выборочно
    python3 run.py --no-monitors                    # без stale/latency мониторов
"""
import argparse
import os
import signal
import subprocess
import sys
import threading
import time

MARKET_DATA_DIR = os.path.join(os.path.dirname(__file__), "market-data")

SCRIPTS = [
    "binance_spot",
    "binance_futures",
    "bybit_spot",
    "bybit_futures",
    "stale_monitor",
    "latency_monitor",
]

MONITORS = {"stale_monitor", "latency_monitor"}

# ANSI colours (отключаются если не TTY)
_USE_COLOR = sys.stdout.isatty()
COLORS = {
    "binance_spot":    "\033[94m",  # синий
    "binance_futures": "\033[96m",  # голубой
    "bybit_spot":      "\033[92m",  # зелёный
    "bybit_futures":   "\033[93m",  # жёлтый
    "stale_monitor":   "\033[95m",  # фиолетовый
    "latency_monitor": "\033[91m",  # красный
}
RESET = "\033[0m"


def colorize(name: str, text: str) -> str:
    if not _USE_COLOR:
        return text
    return f"{COLORS.get(name, '')}{text}{RESET}"


def stream_output(proc: subprocess.Popen, name: str, stop_event: threading.Event):
    """Читает stdout процесса и пишет в общий stdout с префиксом."""
    prefix = colorize(name, f"[{name}]")
    try:
        for line in iter(proc.stdout.readline, b""):
            if stop_event.is_set():
                break
            text = line.decode("utf-8", errors="replace").rstrip()
            print(f"{prefix} {text}", flush=True)
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description="Market Data Collector launcher")
    parser.add_argument(
        "--only", nargs="+", metavar="SCRIPT",
        help=f"Запустить только указанные скрипты. Доступные: {', '.join(SCRIPTS)}"
    )
    parser.add_argument(
        "--no-monitors", action="store_true",
        help="Не запускать stale_monitor и latency_monitor"
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

    print(f"Запуск {len(to_run)} скриптов: {', '.join(to_run)}", flush=True)
    print(f"Рабочая директория: {MARKET_DATA_DIR}", flush=True)
    print(f"Для остановки: Ctrl+C или SIGTERM", flush=True)
    print("-" * 60, flush=True)

    processes: dict[str, subprocess.Popen] = {}
    threads: list[threading.Thread] = []
    stop_event = threading.Event()

    # Запускаем процессы
    for name in to_run:
        script_path = os.path.join(MARKET_DATA_DIR, f"{name}.py")
        if not os.path.exists(script_path):
            print(f"[WARN] {script_path} не найден, пропускаем", file=sys.stderr)
            continue

        env = os.environ.copy()
        env["PYTHONPATH"] = MARKET_DATA_DIR + os.pathsep + env.get("PYTHONPATH", "")

        proc = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # stderr → в тот же поток
            cwd=MARKET_DATA_DIR,
            env=env,
        )
        processes[name] = proc

        t = threading.Thread(target=stream_output, args=(proc, name, stop_event), daemon=True)
        t.start()
        threads.append(t)

        print(f"  Запущен {name} (PID {proc.pid})", flush=True)
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
                    print(f"{prefix} процесс завершился с кодом {rc}", flush=True)
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
        except subprocess.TimeoutExpired:
            print(f"[run.py] {name} не ответил за {grace}с — SIGKILL", flush=True)
            proc.kill()

    stop_event.set()
    for t in threads:
        t.join(timeout=2)

    print("[run.py] Готово.", flush=True)


if __name__ == "__main__":
    main()
