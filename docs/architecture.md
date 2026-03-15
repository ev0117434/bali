# Архитектура системы

## Обзор

```
┌──────────────────────────────────────────────────────────────────┐
│                    dictionaries/main.py                          │
│  (запускается вручную, обновляет списки символов)                │
│                                                                  │
│  Binance REST+WS  Bybit REST+WS  OKX REST+WS  Gate.io REST+WS   │
│        ↓                ↓             ↓              ↓           │
│   combination/      (12 пересечений пар между биржами)           │
│        ↓                                                         │
│   subscribe/        binance/ bybit/ okx/ gate/                   │
└───────────────────────────┬──────────────────────────────────────┘
                            │ читают при старте
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      Биржи (WS)                             │
│     Binance Spot   Binance Futures   Bybit Spot   Bybit Fut │
└────────┬───────────────┬──────────────┬──────────┬──────────┘
         │               │              │          │
         ▼               ▼              ▼          ▼
   binance_spot   binance_futures  bybit_spot  bybit_futures
    (3 conn)        (3 conn)        (3 conn)    (3 conn)
         │               │              │          │
         └───────────────┴──────────────┴──────────┘
                                │
                    Redis Pipeline (HSET + EXPIRE + PUBLISH)
                                │
                          ┌─────▼──────┐
                          │   Redis    │
                          │            │
                          │  md:*:*:*  │  ← Hash keys (TTL 300s)
                          │            │
                          └─────┬──────┘
                                │
               ┌────────────────┼──────────────────┐
               │                │                   │
         stale_monitor   latency_monitor      consumers
          (SCAN+HGET)    (SCAN+HMGET)       (SUBSCRIBE)
```

## Поток данных

```
Биржа WS-сообщение
    │
    ├── ts_received = now_ms()
    │
    ▼
parse_message()           # извлечь bid/ask/symbol/ts_exchange
    │
    ▼
write_redis() — pipeline:
    ├── HSET md:{ex}:{mkt}:{sym}  {8 fields}
    ├── EXPIRE ... TTL
    └── PUBLISH md:updates:{ex}:{mkt}  {"symbol":..., "key":...}
```

## Соединения с биржей

Каждый коллектор открывает несколько параллельных WS-соединений:

```
N символов / SYMBOLS_PER_CONN(150) = K соединений

Binance Spot:    ~270 / 150 = 2 соединения
Binance Futures: ~310 / 150 = 3 соединения
Bybit Spot:      ~310 / 150 = 3 соединения
Bybit Futures:   ~270 / 150 = 2 соединения

Итого: ~10 WS-соединений одновременно
(точное число зависит от текущих subscribe/*.txt)
```

Каждое соединение — отдельная asyncio.Task. Падение одного не влияет на остальные.

## Модуль dictionaries

Запускается вручную (`python3 dictionaries/main.py`) для актуализации списков символов.
Коллекторы `market-data/` читают результат при каждом старте.

### Пайплайн

```
1. REST API (4 биржи параллельно)
      Binance /exchangeInfo
      Bybit   /instruments-info
      OKX     /instruments
      Gate.io /currency_pairs + /contracts
          ↓
   Все USDT/USDC пары → сохранить в {exchange}/data/

2. WebSocket-валидация (4 биржи параллельно, 60 сек)
      Подписка на все пары → наблюдение
      Фиксируем пары, давшие хоть 1 ответ
          ↓
   Активные пары → {exchange}/data/{exchange}_{market}_active.txt

3. Пересечения (12 комбинаций)
      Для каждой пары бирж: A_spot ∩ B_futures
          ↓
   combination/*.txt

4. Subscribe-файлы (8 файлов)
      Из combination-файлов по ключевым словам в имени
          ↓
   subscribe/{exchange}/{exchange}_{market}.txt
```

### Нормализация форматов

| Биржа | Нативный формат | Нормализованный |
|-------|----------------|----------------|
| Binance | `BTCUSDT` | `BTCUSDT` (без изменений) |
| Bybit | `BTCUSDT` | `BTCUSDT` (без изменений) |
| OKX | `BTC-USDT`, `BTC-USDT-SWAP` | `BTCUSDT` |
| Gate.io | `BTC_USDT` | `BTCUSDT` |

OKX и Gate.io хранят нативные символы в отдельных файлах — они нужны для WS-подписки.
Во всех остальных местах (combination, subscribe) используется нормализованный формат.

### Комбинации

12 пересечений: все возможные пары из 4 бирж × 2 рынка:

```
binance_spot ↔ bybit_futures   (2 файла: bsbyf + bysbf)
binance_spot ↔ okx_futures     (2 файла)
binance_spot ↔ gate_futures    (2 файла)
bybit_spot   ↔ okx_futures     (2 файла)
bybit_spot   ↔ gate_futures    (2 файла)
okx_spot     ↔ gate_futures    (2 файла)
```

## Отказоустойчивость

```
Сценарий              Поведение
─────────────────────────────────────────────────────────
WS disconnect         reconnect с backoff (1s → 60s max)
WS recv timeout       reconnect (нет данных 60 сек)
Redis down            буфер deque(1000), запись при recovery
Коллектор упал        run.py логирует, другие продолжают
Символ stale          stale_monitor алерт через 60 сек
```

## Мониторинг

```
stale_monitor ──SCAN──► Redis ──ts_redis──► delta > 60s? ──► PUBLISH alerts:stale
                                                           └──► log WARNING stale_detected

latency_monitor ─SCAN─► Redis ──ts fields──► stats(min/avg/p95) ──► log INFO latency_report
                                          └──► e2e > 1000ms? ──► log WARNING latency_anomaly
                                          └──► e2e > 5000ms? ──► log CRITICAL latency_anomaly
```

## Масштабирование

Текущая архитектура рассчитана на одну машину. При необходимости горизонтального масштабирования:

- **Несколько Redis** — изменить `REDIS_URL` в каждом скрипте. Коллекторы stateless.
- **Разделение символов** — отредактировать `subscribe/*.txt` файлы, запустить несколько экземпляров.
- **Добавить биржу в market-data** — новый коллектор по аналогии с существующими.
- **Добавить биржу в dictionaries** — новая папка `{exchange}/` с `*_pairs.py` и `*_ws.py`, добавить в `COMBINATIONS` и `SUBSCRIBE_MAP` в `main.py`.
