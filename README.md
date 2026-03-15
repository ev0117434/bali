# Bali — система сбора рыночных данных

Сбор best bid/ask с бирж **Binance**, **Bybit**, **OKX**, **Gate.io** через WebSocket с записью в Redis.
Сканирование арбитражных спредов spot↔futures в реальном времени.
Логирование снапшотов по каждому сигналу на протяжении 3500 секунд.

## Структура репозитория

```
bali/
├── run.py                  # Единая точка запуска всех процессов
│
├── market-data/            # WS-коллекторы и мониторы → пишут в Redis
│   ├── common.py
│   ├── binance_spot.py
│   ├── binance_futures.py
│   ├── bybit_spot.py
│   ├── bybit_futures.py
│   ├── okx_spot.py
│   ├── okx_futures.py
│   ├── gate_spot.py
│   ├── gate_futures.py
│   ├── stale_monitor.py
│   ├── latency_monitor.py
│   ├── requirements.txt
│   ├── .env.example
│   └── tests/
│
├── dictionaries/           # Генерация списков символов (запускается вручную)
│   ├── main.py             # Оркестратор: REST → WS-валидация → combination → subscribe
│   ├── binance/
│   ├── bybit/
│   ├── okx/
│   ├── gate/
│   ├── combination/        # 12 пересечений пар между биржами
│   └── subscribe/          # Готовые списки символов ← читают коллекторы
│
├── spread-scanner/         # Сканер спредов + снапшот-логгер
│   ├── spread_scanner.py   # Арбитражный сканер → signals/spread_signals.csv
│   ├── signal_snapshot.py  # Снапшоты по сигналу → signals/{direction}/{symbol}.csv
│   └── tests/
│
├── signals/                # Создаётся при запуске
│   ├── spread_signals.csv              # Все найденные сигналы
│   ├── spread_signals_anomalies.csv    # Сигналы со спредом > 300%
│   └── {direction}/                    # Снапшоты по направлению
│       └── {SYMBOL}_{ts}.csv           # Снапшот конкретного сигнала
│
└── docs/                   # Документация
    ├── architecture.md
    ├── run.md
    └── troubleshooting.md
```

## Быстрый старт

**Требования:** Python 3.11+, Redis 6+

```bash
# 1. Зависимости
pip install -r market-data/requirements.txt

# 2. Конфигурация
cp market-data/.env.example market-data/.env
# Отредактировать REDIS_URL если Redis не на localhost:6379

# 3. Обновить списки символов (первый запуск или раз в неделю)
cd dictionaries && pip install websockets && python3 main.py && cd ..

# 4. Запустить все процессы
python3 run.py
```

Логи пишутся в `logs/` (по одному файлу на скрипт) и одновременно в терминал.

## Компоненты

| Компонент | Описание |
|-----------|---------|
| `market-data/` | WS-коллекторы Binance/Bybit/OKX/Gate.io, мониторы stale и latency |
| `dictionaries/` | Генерация списков пар через REST + WS-валидацию (вручную) |
| `spread-scanner/spread_scanner.py` | Сканирование арбитражных спредов spot↔futures (12 направлений) |
| `spread-scanner/signal_snapshot.py` | Снапшоты цен по каждому сигналу каждые 0.3 сек / 3500 сек |

## Запуск с опциями

```bash
python3 run.py                                       # все 12 процессов
python3 run.py --no-monitors                         # без stale/latency мониторов
python3 run.py --only binance_spot bybit_spot        # выборочно
python3 run.py --only spread_scanner signal_snapshot # только сканер + снапшоты
python3 run.py --logs-dir /var/log/market-data       # своя папка логов
python3 run.py --no-cleanup                          # не очищать Redis и logs/ при старте
```

## Проверка работы

```bash
# Данные коллекторов в Redis
redis-cli HGETALL md:binance:spot:BTCUSDT

# Живые сигналы в канале
redis-cli subscribe ch:spread_signals

# Файлы сигналов
tail -f signals/spread_signals.csv
ls signals/binance_s_bybit_f/

# Тесты
cd market-data && python3 -m pytest tests/ -q
```

## Обновление списков символов

Запускать по мере необходимости (листинги/деlistingi). Занимает ~5 минут.

```bash
cd dictionaries
python3 main.py
```

Результат: обновляются файлы `subscribe/{exchange}/*.txt`, которые коллекторы
читают при следующем перезапуске.
