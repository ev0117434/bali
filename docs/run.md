# run.py — лаунчер

Единая точка запуска всех 12 скриптов.

## Что делает

1. Запускает каждый скрипт как отдельный subprocess.
2. Стримит их stdout/stderr в терминал с цветным префиксом `[script_name]`.
3. Одновременно пишет логи в `logs/<script>.log` с ротацией.
4. При старте очищает Redis (FLUSHDB), удаляет папки `logs/` и `signals/` — с обратным отсчётом `CLEANUP_DELAY` сек (по умолчанию 3).
5. При `Ctrl+C` или `SIGTERM` — рассылает SIGTERM всем дочерним процессам и ждёт их завершения (grace period 10 сек, затем SIGKILL).
6. Если один процесс упал сам — логирует код выхода, остальные продолжают работать.

## Использование

```bash
python3 run.py                                        # все 12 скриптов
python3 run.py --no-monitors                          # без stale_monitor и latency_monitor
python3 run.py --only binance_spot bybit_spot         # выборочно
python3 run.py --only spread_scanner signal_snapshot  # только сканер + снапшоты
python3 run.py --logs-dir /var/log/market-data        # своя папка логов
python3 run.py --no-cleanup                           # не очищать Redis и logs/ при старте
```

## Список процессов

| Скрипт | Расположение | Описание |
|--------|-------------|---------|
| `binance_spot` | market-data/ | WS-коллектор Binance spot |
| `binance_futures` | market-data/ | WS-коллектор Binance futures |
| `bybit_spot` | market-data/ | WS-коллектор Bybit spot |
| `bybit_futures` | market-data/ | WS-коллектор Bybit futures |
| `okx_spot` | market-data/ | WS-коллектор OKX spot |
| `okx_futures` | market-data/ | WS-коллектор OKX futures |
| `gate_spot` | market-data/ | WS-коллектор Gate.io spot |
| `gate_futures` | market-data/ | WS-коллектор Gate.io futures |
| `stale_monitor` | market-data/ | Алерт если символ не обновлялся > 60s |
| `latency_monitor` | market-data/ | Замер e2e задержки WS→Redis |
| `spread_scanner` | spread-scanner/ | Арбитражный сканер, 12 направлений |
| `signal_snapshot` | spread-scanner/ | Снапшоты по сигналу: 0.3с × 3500с |

## Логи

По умолчанию создаётся папка `logs/` рядом с `run.py`:

```
logs/
├── run.log
├── binance_spot.log
├── binance_futures.log
├── bybit_spot.log
├── bybit_futures.log
├── okx_spot.log
├── okx_futures.log
├── gate_spot.log
├── gate_futures.log
├── stale_monitor.log
├── latency_monitor.log
├── spread_scanner.log
└── signal_snapshot.log
```

**Ротация:** 50 МБ на файл, 5 архивных копий → максимум ~300 МБ на скрипт.

Логи дублируются и в терминал — цветом по скрипту:

| Скрипт | Цвет |
|--------|------|
| binance_spot | синий |
| binance_futures | голубой |
| bybit_spot | зелёный |
| bybit_futures | жёлтый |
| okx_spot | белый |
| okx_futures | светло-серый |
| gate_spot | тёмно-зелёный |
| gate_futures | циан |
| stale_monitor | фиолетовый |
| latency_monitor | красный |
| spread_scanner | оранжевый |
| signal_snapshot | пурпурный |

## Что run.py не делает

- **Не перезапускает** упавшие процессы — это задача supervisord/systemd.
- **Не мониторит** здоровье коллекторов — это задача stale_monitor и latency_monitor.
- **Не агрегирует** логи — каждый процесс пишет в свой файл.
