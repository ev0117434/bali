# run.py — лаунчер

Единая точка запуска всех 6 скриптов.

## Что делает

1. Запускает каждый скрипт как отдельный subprocess.
2. Стримит их stdout/stderr в терминал с цветным префиксом `[script_name]`.
3. Одновременно пишет логи в `logs/<script>.log` с ротацией.
4. При `Ctrl+C` или `SIGTERM` — рассылает SIGTERM всем дочерним процессам и ждёт их завершения (grace period 10 сек, затем SIGKILL).
5. Если один процесс упал сам — логирует код выхода, остальные продолжают работать.

## Использование

```bash
python3 run.py                                       # все 6 скриптов
python3 run.py --no-monitors                         # только 4 коллектора
python3 run.py --only binance_spot bybit_spot        # выборочно
python3 run.py --logs-dir /var/log/market-data       # своя папка логов
```

## Логи

По умолчанию создаётся папка `logs/` рядом с `run.py`:

```
logs/
├── run.log              # старт/стоп лаунчера, exit codes процессов
├── binance_spot.log
├── binance_futures.log
├── bybit_spot.log
├── bybit_futures.log
├── stale_monitor.log
└── latency_monitor.log
```

**Ротация:** 50 МБ на файл, 5 архивных копий → максимум ~300 МБ на скрипт.
При ротации старый файл переименовывается в `binance_spot.log.1`, `.2` и т.д.

Логи дублируются и в терминал — цветом по скрипту:

| Скрипт | Цвет |
|--------|------|
| binance_spot | синий |
| binance_futures | голубой |
| bybit_spot | зелёный |
| bybit_futures | жёлтый |
| stale_monitor | фиолетовый |
| latency_monitor | красный |

## Что run.py не делает

- **Не перезапускает** упавшие процессы — это задача supervisord/systemd.
- **Не мониторит** здоровье коллекторов — это задача stale_monitor и latency_monitor.
- **Не агрегирует** логи — каждый процесс пишет в свой файл.
