# Market Data Collector

Система сбора рыночных данных (best bid/ask) с бирж **Binance** и **Bybit** через WebSocket. Данные записываются в Redis в реальном времени.

## Структура репозитория

```
bali/
├── run.py                  # единая точка запуска всех процессов
├── market-data/            # коллекторы и мониторы
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
└── dictionaries/           # генерация и хранение списков символов
    ├── subscribe/          # готовые списки для подписки
    ├── all_pairs/          # полные списки с бирж
    ├── combination/        # пересечения пар между биржами
    ├── scripts/            # утилиты генерации
    └── configs/            # stable id-маппинги
```

## Быстрый старт

**Требования:** Python 3.11+, Redis 6+

```bash
# 1. Зависимости
pip install -r market-data/requirements.txt

# 2. Конфигурация
cp market-data/.env.example market-data/.env
# Отредактировать REDIS_URL если Redis не на localhost

# 3. Запуск
python3 run.py
```

Логи пишутся в `logs/` (по одному файлу на скрипт) и одновременно в терминал.

## Компоненты

| Компонент | Описание |
|-----------|---------|
| [market-data/](market-data/README.md) | WS-коллекторы, мониторы, тесты |
| [dictionaries/](dictionaries/README.md) | Генерация списков символов |

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
