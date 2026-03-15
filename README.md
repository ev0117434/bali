# Bali — система сбора рыночных данных

Сбор best bid/ask с бирж **Binance**, **Bybit** через WebSocket с записью в Redis.
Генерация и актуализация списков торговых пар для 4 бирж: **Binance**, **Bybit**, **OKX**, **Gate.io**.

## Структура репозитория

```
bali/
├── run.py                  # Единая точка запуска всех процессов market-data
│
├── market-data/            # WS-коллекторы и мониторы → пишут в Redis
│   ├── common.py
│   ├── binance_spot.py
│   ├── binance_futures.py
│   ├── bybit_spot.py
│   ├── bybit_futures.py
│   ├── stale_monitor.py
│   ├── latency_monitor.py
│   ├── requirements.txt
│   ├── .env.example
│   └── tests/
│
├── dictionaries/           # Генерация списков символов (запускается вручную)
│   ├── main.py             # Оркестратор: REST → WS-валидация → combination → subscribe
│   ├── binance/            # binance_pairs.py + binance_ws.py
│   ├── bybit/              # bybit_pairs.py + bybit_ws.py
│   ├── okx/                # okx_pairs.py + okx_ws.py
│   ├── gate/               # gate_pairs.py + gate_ws.py
│   ├── combination/        # 12 пересечений пар между биржами
│   └── subscribe/          # Готовые списки символов ← читают коллекторы
│       ├── binance/
│       ├── bybit/
│       ├── okx/
│       └── gate/
│
├── spread-scanner/         # Сканер арбитражных спредов
│   ├── spread_scanner.py
│   └── tests/
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

# 4. Запустить коллекторы
python3 run.py
```

Логи пишутся в `logs/` (по одному файлу на скрипт) и одновременно в терминал.

## Компоненты

| Компонент | Описание | Документация |
|-----------|---------|-------------|
| `market-data/` | WS-коллекторы Binance/Bybit, мониторы stale и latency | [market-data/README.md](market-data/README.md) |
| `dictionaries/` | Генерация списков пар для 4 бирж через REST + WS-валидацию | [dictionaries/README.md](dictionaries/README.md) |
| `spread-scanner/` | Сканирование арбитражных спредов spot↔futures | — |
| `docs/` | Архитектура, run.py, troubleshooting | [docs/](docs/) |

## Запуск с опциями

```bash
python3 run.py                                       # все 6 процессов
python3 run.py --no-monitors                         # только 4 коллектора
python3 run.py --only binance_spot bybit_spot        # выборочно
python3 run.py --logs-dir /var/log/market-data       # своя папка логов
```

## Проверка работы

```bash
# Redis содержит актуальные данные
redis-cli HGETALL md:binance:spot:BTCUSDT

# Все тесты зелёные
cd market-data && python3 -m pytest tests/ -q

# Acceptance check
bash market-data/tests/acceptance_check.sh
```

## Обновление списков символов

Запускать по мере необходимости (листинги/деlistingi). Занимает ~5 минут.

```bash
cd dictionaries
python3 main.py
```

Результат: обновляются файлы `subscribe/{exchange}/*.txt`, которые коллекторы
читают при следующем перезапуске.
